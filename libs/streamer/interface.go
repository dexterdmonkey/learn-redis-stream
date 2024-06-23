package streamer

type Interface interface {
	Consume(topic, consumerGroup, consumerName string) <-chan Message
	Publish(topic, messageID string, message map[string]interface{})
}
