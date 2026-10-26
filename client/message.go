package client

// Message represents an MQTT application message.
type Message struct {
	Topic     string
	Payload   []byte
	QoS       byte
	Retained  bool
	Duplicate bool
}

// MessageHandler is the callback function invoked when a message arrives.
type MessageHandler func(client *Client, msg Message)
