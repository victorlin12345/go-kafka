package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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
		logger:        logger,
	}, nil
}

func (s *saramaSubscriber) Subscribe(ctx context.Context, topic string) (<-chan pubsub.Message, error) {
	handler := newConsumerGroupHandler(ctx, s.output, s.closing)

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

		}
	}()

	return s.output, nil
}

func (s *saramaSubscriber) closingProcess() {
	// cg.Close() do just once
	if err := s.consumerGroup.Close(); err != nil {
		log.Error(err.Error())
	}
	if !s.client.Closed() {
		if err := s.client.Close(); err != nil {
			log.Error(err.Error())
		}
	}
}

func newConsumerGroupHandler(ctx context.Context, output chan pubsub.Message, closing chan struct{}) *consumerGroupHandler {
	return &consumerGroupHandler{ctx: ctx, output: output, closing: closing}
}

type consumerGroupHandler struct {
	ctx     context.Context
	output  chan pubsub.Message
	closing chan struct{}
	wg      sync.WaitGroup
	timeout time.Duration
}

func (h consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.wg.Add(1)
SendToOutput:
	for {
		select {
		case <-h.closing:
			h.wg.Done()
			break SendToOutput
		case <-h.ctx.Done():
			h.wg.Done()
			break SendToOutput
		case kafkaMsg := <-claim.Messages():
			msg := NewMessageBySaramaConsumerMessage(kafkaMsg)
			h.output <- msg
			err := h.waitAckOrNack(sess, kafkaMsg, msg.Acked(), msg.Nacked())
			if err != nil {
				h.wg.Done()
				break SendToOutput
			}
		}
	}

	return nil
}

var ErrCancelAckOrNack error = errors.New("cancel ack or nack")

func (h consumerGroupHandler) waitAckOrNack(
	sess sarama.ConsumerGroupSession,
	msg *sarama.ConsumerMessage,
	acked <-chan struct{},
	nacked <-chan struct{}) error {

	select {
	case <-h.closing:
		return ErrCancelAckOrNack
	case <-h.ctx.Done():
		return ErrCancelAckOrNack
	case <-acked:
		sess.MarkMessage(msg, "")
	case <-nacked:
		// don't mark message
	}

	return nil
}

func (s *saramaSubscriber) Close() error {
	close(s.closing)
	s.wg.Wait()
	log.Info("close")
	return nil
}
