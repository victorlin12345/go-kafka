package main

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/victorlin12345/go-kafka/pkg/pubsub"
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
		logger.Error(err)
	} else {

		logger.Info("start subscribing...")

		msgs, err := s.Subscribe(ctx, topic)
		if err != nil {
			logger.Error(err)
		}

	ConsumeLoop:
		for {
			select {
			case msg := <-msgs:
				if msg == nil {
					break ConsumeLoop
				}
				payload, groupID, partition, offset := getInfo(msg)
				logger.Info(fmt.Sprintf("msg: %s groupId: %s partition: %s offset %s", payload, groupID, partition, offset))
				msg.Ack()
			}
		}
	}
}

func getInfo(msg pubsub.Message) (payload string, groupID string, partition string, offset string) {
	payload = string(msg.GetPayload())
	groupID = msg.GetMetaData()[kafka.KeySaramaGroupID]
	partition = msg.GetMetaData()[kafka.KeySaramaPartition]
	offset = msg.GetMetaData()[kafka.KeySaramaOffset]

	return
}
