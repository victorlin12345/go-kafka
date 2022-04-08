package kafka

import (
	"context"
	"sync"
)

var closedchan = make(chan struct{})

func init() {
	close(closedchan)
}

func NewMessage(ctx context.Context, payload []byte, opts ...messageOption) Message {
	options := messageOptions{
		metadata: make(map[string]string, 0),
	}

	for _, o := range opts {
		o.apply(&options)
	}

	return &message{
		ctx:         ctx,
		ack:         make(chan struct{}),
		nack:        make(chan struct{}),
		ackSentType: unsent,
		Payload:     payload,
		Metadata:    options.metadata,
	}
}

type message struct {
	ctx         context.Context
	ack         chan struct{}
	nack        chan struct{}
	ackMutex    sync.Mutex
	ackSentType ackType // 用於判斷送過與否
	Payload     []byte
	Metadata    map[string]string
	Topic       string
	Partition   int32
	Offset      int64
	Err         error
}

type ackType int

const (
	unsent ackType = iota // 沒有 ack sent
	ack
	nack
)

func (m *message) Ack() bool {
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

func (m *message) Acked() <-chan struct{} {
	return m.ack
}

func (m *message) Nack() bool {
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

func (m *message) Nacked() <-chan struct{} {
	return m.nack
}

func (m *message) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *message) SetContext(ctx context.Context) {
	m.ctx = ctx
}

func (m *message) GetPayload() []byte {
	return m.Payload
}

func (m *message) SetPayload(payload []byte) {
	m.Payload = payload
}

func (m *message) GetMetaData() map[string]string {
	return m.Metadata
}

func (m *message) SetMetaData(d map[string]string) {
	m.Metadata = d
}

func (m *message) GetTopic() string {
	return m.Topic
}

func (m *message) SetTopic(t string) {
	m.Topic = t
}

func (m *message) GetPartition() int32 {
	return m.Partition
}

func (m *message) SetPartition(p int32) {
	m.Partition = p
}

func (m *message) GetOffset() int64 {
	return m.Offset
}

func (m *message) SetOffset(o int64) {
	m.Offset = o
}

func (m *message) GetError() error {
	return m.Err
}

func (m *message) SetError(err error) {
	m.Err = err
}
