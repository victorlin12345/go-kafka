package kafka

import "errors"

// subscriber errors
var (
	SubscriberClosedError       error = errors.New("subscriber close")
	CancelMessageAckOrNackError error = errors.New("cancel ack or nack")
)

// publisher errors
var (
	ProducerClosedError error = errors.New("producer closed")
)
