package kafka

type messageOptions struct {
	uuid      string
	metadata  map[string]string
	topic     string
	partition int32
	offset    int64
	timestamp int64
}

type messageOption interface {
	apply(o *messageOptions)
}

type uuidOption string

func (uo uuidOption) apply(o *messageOptions) {
	o.uuid = string(uo)
}

func WithUUID(uuid string) messageOption {
	return uuidOption(uuid)
}

type metadataOption map[string]string

func (mo metadataOption) apply(o *messageOptions) {
	o.metadata = map[string]string(mo)
}

func WithMetaData(d map[string]string) messageOption {
	return metadataOption(d)
}

type topicOption string

func (to topicOption) apply(o *messageOptions) {
	o.topic = string(to)
}

func WithTopic(t string) messageOption {
	return topicOption(t)
}

type partitionOption int32

func (po partitionOption) apply(o *messageOptions) {
	o.partition = int32(po)
}

func WithPartition(p int32) messageOption {
	return partitionOption(p)
}

type offsetOption int64

func (oo offsetOption) apply(o *messageOptions) {
	o.offset = int64(oo)
}

func WithOffset(o int64) messageOption {
	return offsetOption(o)
}

type timestampOption int64

func (to timestampOption) apply(o *messageOptions) {
	o.timestamp = int64(to)
}

func WithTimestamp(timestamp int64) messageOption {
	return timestampOption(timestamp)
}
