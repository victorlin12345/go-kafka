package kafka

import (
	"context"

	"github.com/Shopify/sarama"
)

const (
	KeySaramaGroupID string = "SaramaMessageGroupID"
)

func NewMessageBySaramaConsumerMessage(ctx context.Context, msg *sarama.ConsumerMessage) Message {
	metadata := make(map[string]string, len(msg.Headers))

	for _, h := range msg.Headers {
		metadata[string(h.Key)] = string(h.Value)
	}

	return &message{
		ctx:       ctx,
		ack:       make(chan struct{}),
		nack:      make(chan struct{}),
		Payload:   msg.Value,
		Metadata:  metadata,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
}

func NewSaramaProducerMessage(topic string, msg Message) (*sarama.ProducerMessage, error) {

	var groupID sarama.Encoder

	headers := make([]sarama.RecordHeader, 0)

	for k, v := range msg.GetMetaData() {
		h := sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		}
		// group id
		if k == KeySaramaGroupID {
			if v == "" {
				groupID = nil
			} else {
				groupID = sarama.ByteEncoder(v)
			}
		}

		headers = append(headers, h)
	}

	return &sarama.ProducerMessage{
		Headers: headers,
		Topic:   topic,
		Value:   sarama.ByteEncoder(msg.GetPayload()),
		Key:     groupID,
	}, nil
}
