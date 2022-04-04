package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/victorlin12345/go-kafka/pkg/pubsub"
)

type saramaPublisher struct {
	config   SaramaPublisherConfig
	producer sarama.SyncProducer
	logger   *log.Logger
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
	}, nil
}

func (p *saramaPublisher) Publish(topic string, messages ...pubsub.Message) error {
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
	if err := p.producer.Close(); err != nil {
		return fmt.Errorf("cannot close Kafka producer:%w", err)
	}
	return nil
}
