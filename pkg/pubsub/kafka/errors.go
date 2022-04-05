package kafka

import "errors"

// config errors
var (
	RequireBrokersError       error = errors.New("require brokers")
	RequireConsumerGroupError error = errors.New("require consumer group")
)

// subscriber errors
var (
	SubscriberClosedError       error = errors.New("subscriber close")
	CancelMessageAckOrNackError error = errors.New("cancel ack or nack")
)

// publisher errors
var (
	ProducerClosedError error = errors.New("producer closed")
)
