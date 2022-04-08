package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/victorlin12345/go-kafka/pkg/kafka"
)

func main() {
	ctx := context.Background()

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
	defer p.Close()

	// Ctrl C to graceful shutdown
	ch := make(chan os.Signal, 1)
	go func() {
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		p.Close()
	}()

	if err != nil {
		logger.Error(err)
	} else {
		logger.Info("start publishing...")

		topic := "test"
	PublishLoop:
		for i := 0; ; i++ {
			msg := fakeData(ctx, i)
			res, err := p.Publish(ctx, topic, msg)
			if err != nil {
				if err == kafka.ProducerClosedError {
					break PublishLoop
				}
				logger.Error(err)
				break PublishLoop
			}

			logger.Info(fmt.Sprintf("partition: %d offset: %d send msg: %s", res.GetPartition(), res.GetOffset(), string(res.GetPayload())))

			time.Sleep(1 * time.Second)
		}
	}
}

func fakeData(ctx context.Context, i int) kafka.Message {
	groupID := "user1"
	metadata := make(map[string]string, 0)
	metadata[kafka.KeySaramaGroupID] = groupID // to let same groupID message go to same partition
	payload := []byte(fmt.Sprintf("data%d", i))

	return kafka.NewMessage(ctx, payload, kafka.WithMetaData(metadata))
}
