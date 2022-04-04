package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/victorlin12345/go-kafka/pkg/pubsub"
)

type saramaPublisher struct {
	config   SaramaPublisherConfig
	producer sarama.SyncProducer
	logger   *log.Logger
	closing  chan struct{}
	closed   bool
}

// NewPublisher creates a new Kafka Publisher.
func NewSaramaPublisher(config SaramaPublisherConfig, logger *log.Logger) (pubsub.Publisher, error) {
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
		closing:  make(chan struct{}),
	}, nil
}

var ErrSaramaProducerClosed = errors.New("sarama producer closed")

func (p *saramaPublisher) Publish(ctx context.Context, topic string, messages ...pubsub.Message) error {
	if p.closed {
		return ErrSaramaProducerClosed
	}

	select {
	case <-p.closing:
		return ErrSaramaProducerClosed
	case <-ctx.Done():
		return ErrSaramaProducerClosed
	default:
	}

	for _, msg := range messages {
		kafkaMsg, err := NewSaramaProducerMessage(topic, msg)
		if err != nil {
			return fmt.Errorf("cannot marshal to kafka message:%w", err)
		}

		partition, offset, err := p.producer.SendMessage(kafkaMsg)
		if err != nil {
			return fmt.Errorf("cannot produce message:%w", err)
		}
		p.logger.Trace("partition:", partition, "offset:", offset)
	}

	return nil
}

func (p *saramaPublisher) Close() error {
	if p.closed {
		return nil
	}
	close(p.closing)
	p.closed = true

	if err := p.producer.Close(); err != nil {
		return fmt.Errorf("cannot close Kafka producer:%w", err)
	}

	p.logger.Info("publisher closed")
	return nil
}
