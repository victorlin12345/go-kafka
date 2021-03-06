package kafka

import "context"

type Publisher interface {
	Publish(ctx context.Context, topic string, message Message) (msg Message, closed error)
	Close() error
}
type Subscriber interface {
	Subscribe(ctx context.Context, topic string) (msgs <-chan Message, closed error)
	Close() error
}

type Message interface {
	Context() context.Context
	SetContext(ctx context.Context)

	// Acknowledgment
	Ack() bool
	Acked() <-chan struct{}

	// Negative-Acknowledgment
	Nack() bool
	Nacked() <-chan struct{}

	GetUUID() string
	SetUUID(uuid string)

	GetPayload() []byte
	SetPayload(b []byte)

	GetMetaData() map[string]string
	SetMetaData(mp map[string]string)

	// version usage
	GetTimestamp() int64
	SetTimestamp(version int64)

	GetTopic() string
	SetTopic(topic string)
	GetPartition() int32
	SetPartition(partition int32)
	GetOffset() int64
	SetOffset(offset int64)

	GetError() error
	SetError(err error)
}
