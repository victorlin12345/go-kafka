package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type saramaPublisher struct {
	config   SaramaPublisherConfig
	producer sarama.SyncProducer
	logger   *log.Logger
	closed   bool
	lock     sync.Mutex
}

// NewPublisher creates a new Kafka Publisher.
func NewSaramaPublisher(config SaramaPublisherConfig, logger *log.Logger) (Publisher, error) {
	config.setDefault()

	if err := config.validate(); err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(config.Brokers, config.OverwriteSaramaConfig)
	if err != nil {
		return nil, fmt.Errorf("cannot create Kafka producer:%w", err)
	}

	return &saramaPublisher{
		config:   config,
		producer: producer,
		logger:   logger,
	}, nil
}

func (p *saramaPublisher) Publish(ctx context.Context, topic string, message Message) (Message, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return nil, ProducerClosedError
	}

	// for context cancel
	select {
	case <-ctx.Done():
		p.logger.Info("publisher closed")
		return nil, ProducerClosedError
	default:
	}

	kafkaMsg, err := NewSaramaProducerMessage(topic, message)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal to kafka message:%w", err)
	}

	partition, offset, err := p.producer.SendMessage(kafkaMsg)
	if err != nil {
		return nil, fmt.Errorf("cannot produce message:%w", err)
	}

	message.SetTopic(topic)
	message.SetPartition(partition)
	message.SetOffset(offset)

	p.logger.Trace("topic:", topic, " partition:", partition, " offset:", offset)

	return message, nil
}

func (p *saramaPublisher) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.producer.Close(); err != nil {
		return fmt.Errorf("cannot close Kafka producer:%w", err)
	}

	p.logger.Info("publisher closed")
	return nil
}
