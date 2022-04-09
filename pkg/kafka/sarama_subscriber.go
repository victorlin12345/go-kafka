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
	config         SaramaSubscriberConfig
	client         sarama.Client
	consumerGroup  sarama.ConsumerGroup
	output         chan Message
	closing        chan struct{}
	closed         bool
	consumeWg      *sync.WaitGroup
	consumeErrorWg *sync.WaitGroup
	handlerWg      *sync.WaitGroup
	lock           sync.Mutex
	logger         *log.Logger
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
		config:         config,
		client:         client,
		consumerGroup:  consumerGroup,
		output:         make(chan Message, 0),
		closing:        make(chan struct{}),
		consumeWg:      new(sync.WaitGroup),
		consumeErrorWg: new(sync.WaitGroup),
		handlerWg:      new(sync.WaitGroup),
		logger:         logger,
	}, nil
}

func (s *saramaSubscriber) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return nil, SubscriberClosedError
	}

	handler := newConsumerGroupHandler(ctx, s)

	// start consuming
	go s.consumingProcess(ctx, topic, handler)

	// listen consumer group error, and throw message with error
	go s.handleConsumeError(ctx, handler)

	// graceful shutdown
	go func() {
		s.consumeWg.Wait()
		select {
		case <-s.closing:
			s.closingProcess()
		case <-ctx.Done():
			s.closingProcess()
		}
	}()

	return s.output, nil
}

func (s *saramaSubscriber) consumingProcess(ctx context.Context, topic string, handler *consumerGroupHandler) {
	defer s.consumeWg.Done()
	s.consumeWg.Add(1)

ReconnectLoop:
	for {
		// consumerGroup.Consume need to run in an infinte loop (ReconnectLoop)
		// for server-side rebalance happens.
		err := s.consumerGroup.Consume(ctx, []string{topic}, handler)
		if err != nil {
			log.Error(err.Error())
		}

		select {
		case <-s.closing:
			s.consumeErrorWg.Wait()
			s.logger.Info("leave reconnect loop (close called)")
			break ReconnectLoop

		case <-ctx.Done():
			s.consumeErrorWg.Wait()
			s.logger.Info("leave reconnect loop (context cancelled)")
			break ReconnectLoop
		default:
			// pass
		}

		// default: 1 second to retry
		time.Sleep(s.config.ReconnectRetrySleep)
		s.logger.Info("reconnecting")
	}
}

func (s *saramaSubscriber) handleConsumeError(ctx context.Context, handler *consumerGroupHandler) {
	defer s.consumeErrorWg.Done()
	s.consumeErrorWg.Add(1)

	// when consume life-cycle occur error, consumerGroup.Errors return errs
	errs := s.consumerGroup.Errors()
ConsumeErrorLoop:
	for {
		select {
		case <-s.closing:
			s.handlerWg.Wait()
			s.logger.Info("leave consume error loop (close called)")
			break ConsumeErrorLoop

		case <-ctx.Done():
			s.handlerWg.Wait()
			s.logger.Info("leave consume group error loop (context cancelled)")
			break ConsumeErrorLoop
		case err := <-errs:
			if err == nil {
				continue
			}
			msg := NewMessage(ctx, []byte("errors occurs during consumer life-cycle"))
			msg.SetError(err)
			s.output <- msg
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

func newConsumerGroupHandler(ctx context.Context, s *saramaSubscriber) *consumerGroupHandler {
	return &consumerGroupHandler{
		ctx:     ctx,
		output:  s.output,
		closing: s.closing,
		wg:      s.handlerWg,
		logger:  s.logger,
	}
}

type consumerGroupHandler struct {
	ctx     context.Context
	output  chan Message
	closing chan struct{}
	wg      *sync.WaitGroup
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
			h.logger.Info("stop consume claim (close called)")
			break SendToOutput

		case <-h.ctx.Done():
			h.logger.Info("stop consume claim (context cancelled)")
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
		h.logger.Info("cancel wait ack or nack (close called)")
		return CancelMessageAckOrNackError
	case <-h.ctx.Done():
		h.logger.Info("cancel wait ack or nack (context cancelled)")
		return CancelMessageAckOrNackError
	case <-acked:
		sess.MarkMessage(msg, "")
	case <-nacked:
		// don't mark message
	}

	return nil
}

func (s *saramaSubscriber) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return nil
	}
	close(s.closing)
	s.closed = true

	return nil
}
