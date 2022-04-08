package kafka

import (
	"strconv"

	"github.com/Shopify/sarama"
)

const (
	KeySaramaTopic     string = "SaramaMessageTopic"
	KeySaramaGroupID   string = "SaramaMessageGroupID"
	KeySaramaPartition string = "SaramaMessagePartition"
	KeySaramaOffset    string = "SaramaMessageOffset"
	KeySaramaTimestamp string = "SaramaMessageTimestamp"
)

func NewMessageBySaramaConsumerMessage(msg *sarama.ConsumerMessage) Message {
	metadata := make(map[string]string, len(msg.Headers))
	metadata[KeySaramaTopic] = msg.Topic
	metadata[KeySaramaPartition] = strconv.FormatInt(int64(msg.Partition), 10)
	metadata[KeySaramaOffset] = strconv.FormatInt(msg.Offset, 10)
	metadata[KeySaramaTimestamp] = msg.Timestamp.String()

	for _, h := range msg.Headers {
		metadata[string(h.Key)] = string(h.Value)
	}

	return &message{
		Payload:  msg.Value,
		Metadata: metadata,
		ack:      make(chan struct{}),
		nack:     make(chan struct{}),
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
