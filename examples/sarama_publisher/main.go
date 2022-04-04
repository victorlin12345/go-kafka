package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/victorlin12345/go-kafka/pkg/pubsub/kafka"
)

func init() {

}

func main() {

	ctx := context.Background()

	topic := "test"

	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Retry.Max = 10
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner // keep order right with same groupKey of messages

	config := kafka.SaramaPublisherConfig{
		Brokers:               []string{"localhost:9091", "localhost:9092"},
		OverwriteSaramaConfig: saramaConfig,
	}

	logger := log.New()

	p, err := kafka.NewSaramaPublisher(config, logger)
	if err != nil {
		logger.Error(err.Error())
	}

	logger.Info("start publishing...")

PublishLoop:
	for i := 0; ; i++ {
		groupID := "user1"
		metadata := make(map[string]string, 0)
		metadata[kafka.KeySaramaGroupID] = groupID // to let same groupID message go to same partition
		payload := []byte(fmt.Sprintf("data%d", i))

		msg := kafka.NewMessage(ctx, payload, metadata)

		err := p.Publish(topic, msg)
		if err != nil {
			logger.Error(err)
			break PublishLoop
		}

		logger.Info("send msg: ", string(msg.Payload))

		time.Sleep(1 * time.Second)
	}

	p.Close()
}
