package streamer

type Message struct {
	ID     string
	Values map[string]interface{}
	Error  error
}
