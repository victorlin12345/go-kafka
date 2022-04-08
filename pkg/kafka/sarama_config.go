package kafka

import (
	"time"

	"github.com/Shopify/sarama"
)

type SaramaSubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config

	// Kafka consumer group (required, can't be empty string)
	ConsumerGroup string

	// Due to consumerGroup.Consume need to live in infinite loop. We setup a retry interval for it.
	ReconnectRetrySleep time.Duration
}

func (c *SaramaSubscriberConfig) setDefault() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaSubscriberConfig()
	}

	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}
}

func (c SaramaSubscriberConfig) validate() error {
	if len(c.Brokers) == 0 {
		return RequireBrokersError
	}

	if c.ConsumerGroup == "" {
		return RequireConsumerGroupError
	}

	return nil
}

func DefaultSaramaSubscriberConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true
	return config
}

type SaramaPublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config
}

func (c *SaramaPublisherConfig) setDefault() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaPublisherConfig()
	}
}

func DefaultSaramaPublisherConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Version = sarama.V1_0_0_0
	config.Metadata.Retry.Backoff = time.Second * 2

	return config
}

func (c SaramaPublisherConfig) validate() error {
	if len(c.Brokers) == 0 {
		return RequireBrokersError
	}

	return nil
}
