package pubsub

import "context"

type Publisher interface {
	Publish(topic string, messages ...Message) error
	Close() error
}
type Subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan Message, error)
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

	GetPayload() []byte
	SetPayload(b []byte)

	GetMetaData() map[string]string
	SetMetaData(mp map[string]string)
}
