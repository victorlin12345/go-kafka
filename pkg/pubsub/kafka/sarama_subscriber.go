package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/victorlin12345/go-kafka/pkg/pubsub"
)

type saramaSubscriber struct {
	config        SaramaSubscriberConfig
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	output        chan pubsub.Message
	closing       chan struct{}
	closed        bool
	wg            sync.WaitGroup
	logger        *log.Logger
}

func NewSaramaSubscriber(config SaramaSubscriberConfig, logger *log.Logger) (*saramaSubscriber, error) {
	config.setDefault()

	if err := config.validate(); err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(config.Brokers, config.OverwriteSaramaConfig)
	if err != nil {
		return nil, fmt.Errorf("fail to create client:%w", err)
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(config.ConsumerGroup, client)
	if err != nil {
		return nil, fmt.Errorf("fail to create consumer group:%w", err)
	}

	return &saramaSubscriber{
		config:        config,
		client:        client,
		consumerGroup: consumerGroup,
		output:        make(chan pubsub.Message, 0),
		closing:       make(chan struct{}),
		logger:        logger,
	}, nil
}

func (s *saramaSubscriber) Subscribe(ctx context.Context, topic string) (<-chan pubsub.Message, error) {
	if s.closed {
		return nil, SubscriberClosedError
	}

	handler := newConsumerGroupHandler(ctx, s.output, s.closing, s.logger)

	s.wg.Add(1)

	// start consuming
	go func() {
		err := s.consumerGroup.Consume(ctx, []string{topic}, handler)
		if err != nil {
			log.Error(err.Error())
			close(s.closing)
		}
	}()

	// graceful shutdown
	go func() {
		handler.wg.Wait()

		select {
		case <-s.closing:
			s.closingProcess()
			s.wg.Done()

		case <-ctx.Done():
			s.closingProcess()
			log.Info("subscriber closed")
		}
	}()

	return s.output, nil
}

func (s *saramaSubscriber) closingProcess() {
	// consumerGroup.Close() do just once
	if err := s.consumerGroup.Close(); err != nil {
		s.logger.Error(err)
	}
	s.logger.Info("consumer group closed")

	if !s.client.Closed() {
		if err := s.client.Close(); err != nil {
			s.logger.Error(err)
		}
	}
	s.logger.Info("client closed")

	close(s.output)
	s.logger.Info("output closed")
}

func newConsumerGroupHandler(ctx context.Context,
	output chan pubsub.Message,
	closing chan struct{},
	logger *log.Logger,
) *consumerGroupHandler {
	return &consumerGroupHandler{
		ctx:     ctx,
		output:  output,
		closing: closing,
		logger:  logger,
	}
}

type consumerGroupHandler struct {
	ctx     context.Context
	output  chan pubsub.Message
	closing chan struct{}
	wg      sync.WaitGroup
	logger  *log.Logger
}

func (h consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.wg.Add(1)
	defer h.wg.Done()

SendToOutput:
	for {
		select {
		case <-h.closing:
			h.logger.Info("stop consume claim")
			break SendToOutput

		case <-h.ctx.Done():
			h.logger.Info("stop consume claim")
			break SendToOutput

		case kafkaMsg, ok := <-claim.Messages():
			if !ok {
				break SendToOutput
			}

			msg := NewMessageBySaramaConsumerMessage(kafkaMsg)
			h.output <- msg

			if err := h.waitAckOrNack(sess, kafkaMsg, msg.Acked(), msg.Nacked()); err != nil {
				break SendToOutput
			}
		}
	}

	return nil
}

func (h consumerGroupHandler) waitAckOrNack(
	sess sarama.ConsumerGroupSession,
	msg *sarama.ConsumerMessage,
	acked <-chan struct{},
	nacked <-chan struct{}) error {
	select {
	case <-h.closing:
		return CancelMessageAckOrNackError
	case <-h.ctx.Done():
		return CancelMessageAckOrNackError
	case <-acked:
		sess.MarkMessage(msg, "")
	case <-nacked:
		// don't mark message
	}

	return nil
}

func (s *saramaSubscriber) Close() error {
	if s.closed {
		return nil
	}
	close(s.closing)
	s.closed = true

	s.wg.Wait()
	log.Info("subscriber closed")
	return nil
}
