package main

import (
	"context"
	"fmt"

	"github.com/victorlin12345/go-kafka/pkg/pubsub/kafka"
)

func main() {
	ctx := context.Background()

	config := kafka.SaramaSubscriberConfig{
		Brokers:       []string{"localhost:9091", "localhost:9092"},
		ConsumerGroup: "Group2",
	}

	s := kafka.NewSaramaSubscriber(config)
	topic := "test"
	msgs, err := s.Subscribe(ctx, topic)
	if err != nil {
		fmt.Println(err)
	}
	for {
		select {
		case msg := <-msgs:
			fmt.Println("XXX", string(msg.GetPayload()))
			msg.Ack()
		}
	}
}
