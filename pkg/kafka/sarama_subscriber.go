package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type saramaSubscriber struct {
	config        SaramaSubscriberConfig
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	output        chan Message
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
		output:        make(chan Message, 0),
		closing:       make(chan struct{}),
		logger:        logger,
	}, nil
}

func (s *saramaSubscriber) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	if s.closed {
		return nil, SubscriberClosedError
	}

	handler := newConsumerGroupHandler(ctx, s.output, s.closing, s.logger)

	s.wg.Add(1)

	// start consuming
	go s.consumingProcess(ctx, topic, handler)

	go s.handleConsumeError(ctx)

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

func (s *saramaSubscriber) consumingProcess(ctx context.Context, topic string, handler *consumerGroupHandler) {
	// consumerGroup.Consume need to run in an infinte loop (ReconnectLoop)
	// for server-side rebalance happens.
ReconnectLoop:
	for {
		select {
		case <-s.closing:
			handler.wg.Wait()
			s.logger.Info("close called, leave reconnect loop")
			break ReconnectLoop

		case <-ctx.Done():
			handler.wg.Wait()
			s.logger.Info("context cancelled, leave reconnect loop")
			break ReconnectLoop
		default:
			// pass
		}

		err := s.consumerGroup.Consume(ctx, []string{topic}, handler)
		if err != nil {
			log.Error(err.Error())
		}
		// default: 1 second to retry
		time.Sleep(s.config.ReconnectRetrySleep)
		s.logger.Info("reconnecting")
	}
}

func (s *saramaSubscriber) handleConsumeError(ctx context.Context) {
	errs := s.consumerGroup.Errors()
ErrorLoop:
	for {
		select {
		case <-ctx.Done():
			break ErrorLoop
		case <-s.closing:
			break ErrorLoop
		case err := <-errs:
			if err == nil {
				continue
			}
			msg := NewMessage(ctx, []byte("error"))
			msg.SetError(err)
			s.output <- msg
			// s.logger.Error("sarama internal error:", err)
		}
	}
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
	output chan Message,
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
	output  chan Message
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
			h.logger.Info("close called, stop consume claim")
			break SendToOutput

		case <-h.ctx.Done():
			h.logger.Info("context cancelled, stop consume claim")
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
		h.logger.Info("close called, cancel wait ack or nack")
		return CancelMessageAckOrNackError
	case <-h.ctx.Done():
		h.logger.Info("context cancelled, cancel wait ack or nack")
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
