package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"log"

	"github.com/Shopify/sarama"
	"github.com/victorlin12345/go-kafka/pkg/pubsub"
)

type saramaSubscriber struct {
	config  SaramaSubscriberConfig
	output  chan pubsub.Message
	closing chan struct{}
	wg      sync.WaitGroup
}

func NewSaramaSubscriber(config SaramaSubscriberConfig) *saramaSubscriber {
	if config.OverwriteSaramaConfig == nil {
		config.setDefault()
	}
	return &saramaSubscriber{
		config: config,
		output: make(chan pubsub.Message, 0),
	}
}

func (s *saramaSubscriber) Subscribe(ctx context.Context, topic string) (<-chan pubsub.Message, error) {
	c, err := sarama.NewClient(s.config.Brokers, s.config.OverwriteSaramaConfig)
	if err != nil {
		return nil, fmt.Errorf("fail to create client:%w", err)
	}

	if s.config.ConsumerGroup == "" {
		return nil, fmt.Errorf("consumer group is nil")
	}

	cg, err := sarama.NewConsumerGroupFromClient(s.config.ConsumerGroup, c)
	if err != nil {
		return nil, fmt.Errorf("fail to create consumer group:%w", err)
	}

	handler := newConsumerGroupHandler(ctx, s.output, s.closing)

	s.wg.Add(1)

	// start consuming
	go func() {
		err := cg.Consume(ctx, []string{topic}, handler)
		if err != nil {
			log.Fatal(err)
			close(s.closing)
		}
	}()

	// graceful shutdown
	go func() {
		handler.wg.Wait()

		select {
		case <-s.closing:
			s.closingProcess(c, cg)
		case <-ctx.Done():
			s.closingProcess(c, cg)
		}
	}()

	return s.output, nil
}

func (s *saramaSubscriber) closingProcess(c sarama.Client, cg sarama.ConsumerGroup) {
	// cg.Close() do just once
	if err := cg.Close(); err != nil {
		log.Fatalln(err)
	}
	if !c.Closed() {
		if err := c.Close(); err != nil {
			log.Fatalln(err)
		}
	}
	s.wg.Done()
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
	fmt.Println("closed")
	return nil
}
