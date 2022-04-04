package kafka

import (
	"context"
	"sync"
)

var closedchan = make(chan struct{})

func init() {
	close(closedchan)
}

func NewMessage(ctx context.Context, payload []byte, metadata map[string]string) *Message {
	return &Message{
		ctx:         ctx,
		ack:         make(chan struct{}),
		nack:        make(chan struct{}),
		ackSentType: unsent,
		Payload:     payload,
		Metadata:    metadata,
	}
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

func (m *Message) GetMetaData() map[string]string {
	return m.Metadata
}

func (m *Message) SetMetaData(mp map[string]string) {
	m.Metadata = mp
}
