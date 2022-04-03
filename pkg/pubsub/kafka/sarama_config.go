package kafka

import "github.com/Shopify/sarama"

type SaramaSubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config

	// Kafka consumer group (required, can't be empty string)
	ConsumerGroup string
}

func (c *SaramaSubscriberConfig) setDefault() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaSubscriberConfig()
	}
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
