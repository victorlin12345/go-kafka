package main

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/victorlin12345/go-kafka/pkg/pubsub/kafka"
)

func main() {
	ctx := context.Background()

	topic := "test"

	config := kafka.SaramaSubscriberConfig{
		Brokers:       []string{"localhost:9091", "localhost:9092"},
		ConsumerGroup: "cg1",
	}

	logger := log.New()

	s, err := kafka.NewSaramaSubscriber(config, logger)
	if err != nil {
		err = fmt.Errorf("fail to new sarama subscriber:%w", err)
		logger.Error(err.Error())
	}

	log.Info("start subscribing...")

	msgs, err := s.Subscribe(ctx, topic)
	if err != nil {
		err = fmt.Errorf("fail subscribing:%w", err)
		log.Error(err.Error())
	}
	for {
		select {
		case msg := <-msgs:
			payload := string(msg.GetPayload())
			groupID := msg.GetMetaData()[kafka.KeySaramaGroupID]
			partition := msg.GetMetaData()[kafka.KeySaramaPartition]
			offset := msg.GetMetaData()[kafka.KeySaramaOffset]

			logger.Info(fmt.Sprintf("msg: %s groupId: %s partition: %s offset %s", payload, groupID, partition, offset))
			msg.Ack()
		}
	}
}
