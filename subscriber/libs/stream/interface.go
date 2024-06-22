package stream

type Interface interface {
	Consume(stream, consumerGroup, consumerName string) <-chan Message
	Publish(stream, messageID string, message map[string]interface{})
}
