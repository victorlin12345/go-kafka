package kafka

type messageOptions struct {
	metadata  map[string]string
	topic     string
	partition int32
	offset    int64
}

type messageOption interface {
	apply(o *messageOptions)
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
