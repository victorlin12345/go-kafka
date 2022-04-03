package kafka

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/victorlin12345/go-kafka/pkg/pubsub"
)

func NewMessageBySaramaConsumerMessage(msg *sarama.ConsumerMessage) pubsub.Message {
	metadata := make(map[string]string, len(msg.Headers))

	for _, h := range msg.Headers {
		metadata[string(h.Key)] = string(h.Value)
	}

	return &Message{
		Payload:  msg.Value,
		Metadata: metadata,
		ack:      make(chan struct{}),
		nack:     make(chan struct{}),
	}
}

var closedchan = make(chan struct{})

func init() {
	close(closedchan)
}

type Message struct {
	ctx         context.Context
	ack         chan struct{}
	nack        chan struct{}
	ackMutex    sync.Mutex
	ackSentType ackType // 用於判斷送過與否
	Payload     []byte
	Metadata    map[string]string
}

type ackType int

const (
	unsent ackType = iota // 沒有 ack sent
	ack
	nack
)

func (m *Message) Ack() bool {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.ackSentType == nack {
		return false
	}
	// 已送過
	if m.ackSentType != unsent {
		return true
	}

	m.ackSentType = ack
	if m.ack == nil {
		m.ack = closedchan
	} else {
		close(m.ack)
	}

	return true

}

func (m *Message) Acked() <-chan struct{} {
	return m.ack
}

func (m *Message) Nack() bool {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.ackSentType == ack {
		return false
	}
	// 已送過
	if m.ackSentType != unsent {
		return true
	}

	m.ackSentType = nack
	if m.nack == nil {
		m.nack = closedchan
	} else {
		close(m.nack)
	}

	return true
}

func (m *Message) Nacked() <-chan struct{} {
	return m.nack
}

func (m *Message) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *Message) SetContext(ctx context.Context) {
	m.ctx = ctx
}

func (m *Message) GetPayload() []byte {
	return m.Payload
}

func (m *Message) SetPayload(payload []byte) {
	m.Payload = payload
}

func (m *Message) GetMetaData(k string) string {
	if v, ok := m.Metadata[k]; ok {
		return v
	}
	return ""
}

func (m *Message) SetMetaData(k string, v string) {
	m.Metadata[k] = v
}
