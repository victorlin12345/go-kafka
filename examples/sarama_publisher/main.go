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
	logger := log.New()

	config := kafka.SaramaPublisherConfig{
		Brokers:               []string{"localhost:9091", "localhost:9092"},
		OverwriteSaramaConfig: saramaOverwriteConfig(),
	}

	p, err := kafka.NewSaramaPublisher(config, logger)
	if err != nil {
		logger.Error(err)
	} else {
		go publishProcess(ctx, logger, "test", p)
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	logger.Info("shutdown process start")

	if err := p.Close(); err != nil {
		logger.Error(fmt.Sprintf("shutdown process failed:%+v", err))
	}

}

func publishProcess(ctx context.Context, logger *log.Logger, topic string, publisher kafka.Publisher) {
	logger.Info("start publishing...")

PublishLoop:
	for i := 0; ; i++ {
		msg := fooData(ctx, i)
		res, err := publisher.Publish(ctx, topic, msg)
		if err != nil {
			logger.Error(err)
			break PublishLoop
		}

		logger.Info(fmt.Sprintf("partition: %d offset: %d send msg: %s", res.GetPartition(), res.GetOffset(), string(res.GetPayload())))

		time.Sleep(1 * time.Second)
	}
}

func saramaOverwriteConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Retry.Max = 10
	saramaConfig.Producer.Return.Successes = true
	// keep order right with same groupKey of messages
	saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner

	return saramaConfig
}

func fooData(ctx context.Context, i int) kafka.Message {
	groupID := "user1"
	metadata := make(map[string]string, 0)
	metadata[kafka.KeySaramaGroupID] = groupID // to let same groupID message go to same partition
	payload := []byte(fmt.Sprintf("foo%d", i))

	return kafka.NewMessage(ctx, payload, kafka.WithMetaData(metadata))
}
