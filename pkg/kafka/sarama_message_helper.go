package kafka

import (
	"context"
	"strconv"

	"github.com/Shopify/sarama"
)

const (
	KeySaramaUUID      string = "SaramaMessageUUID"
	KeySaramaTimestamp string = "SaramaMessageTimestamp"
	KeySaramaGroupID   string = "SaramaMessageGroupID"
)

func NewMessageBySaramaConsumerMessage(ctx context.Context, msg *sarama.ConsumerMessage) Message {
	metadata := make(map[string]string, len(msg.Headers))

	for _, h := range msg.Headers {
		metadata[string(h.Key)] = string(h.Value)
	}

	var uuid string
	var timestamp int64

	if v, ok := metadata[KeySaramaUUID]; ok {
		uuid = v
	}
	if v, ok := metadata[KeySaramaTimestamp]; ok {
		timestamp, _ = strconv.ParseInt(v, 10, 64)
	}

	return &message{
		ctx:       ctx,
		ack:       make(chan struct{}),
		nack:      make(chan struct{}),
		uuid:      uuid,
		Payload:   msg.Value,
		Metadata:  metadata,
		timestamp: timestamp,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
}

func NewSaramaProducerMessage(topic string, msg Message) (*sarama.ProducerMessage, error) {

	var groupID sarama.Encoder

	headers := make([]sarama.RecordHeader, 0)

	// uuid
	headers = append(headers, sarama.RecordHeader{
		Key:   sarama.ByteEncoder(KeySaramaUUID),
		Value: sarama.ByteEncoder(msg.GetUUID()),
	})

	// timestamp
	headers = append(headers, sarama.RecordHeader{
		Key:   sarama.ByteEncoder(KeySaramaTimestamp),
		Value: sarama.ByteEncoder(strconv.FormatInt(msg.GetTimestamp(), 10)),
	})

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
