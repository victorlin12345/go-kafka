package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/victorlin12345/go-kafka/pkg/kafka"
)

func main() {
	ctx := context.Background()
	logger := log.New()

	config := kafka.SaramaSubscriberConfig{
		Brokers:       []string{"localhost:9091", "localhost:9092"},
		ConsumerGroup: "cg1",
	}

	s, err := kafka.NewSaramaSubscriber(config, logger)
	if err != nil {
		logger.Error(err)
	} else {
		go subscribeProcess(ctx, logger, "test", s)
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	logger.Info("shutdown process start")

	if err := s.Close(); err != nil {
		logger.Error(fmt.Sprintf("shutdown process failed:%+v", err))
	}
}

func subscribeProcess(ctx context.Context, logger *log.Logger, topic string, subscriber kafka.Subscriber) {
	logger.Info("start subscribing...")

	msgs, err := subscriber.Subscribe(ctx, topic)
	if err != nil {
		logger.Error(err)
	}

MessageLoop:
	for {
		select {
		case msg := <-msgs:
			// message channel be closed
			if msg == nil {
				break MessageLoop
			}
			// occur error when consuming
			if err := msg.GetError(); err != nil {
				logger.Error(err)
			} else {
				printMsgInfo(msg, logger)
				msg.Ack()
			}
		}
	}
}

func printMsgInfo(msg kafka.Message, logger *log.Logger) {
	payload := string(msg.GetPayload())
	groupID := msg.GetMetaData()[kafka.KeySaramaGroupID]
	partition := msg.GetPartition()
	offset := msg.GetOffset()
	msgInfo := fmt.Sprintf("msg: %s groupId: %s partition: %d offset %d", payload, groupID, partition, offset)
	logger.Info(msgInfo)
}
