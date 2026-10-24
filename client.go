package mqttOQ

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/hphuc15/mqttOQ/packet"
	"github.com/quic-go/quic-go"
)

type Client struct {
	// Connect() error
	// Publish() error
	clientID   string
	keepAlive  time.Duration
	tlsConfig  *tls.Config
	quicConfig *quic.Config
	conn       *quic.Conn
	stream     *quic.Stream
}

func NewClient(clientID string, keepAlive time.Duration, tlsConfig *tls.Config) *Client {
	cfg := &tls.Config{}
	if tlsConfig != nil {
		cfg = tlsConfig.Clone()
	}
	cfg.NextProtos = []string{"mqtt"}

	return &Client{
		clientID:  clientID,
		keepAlive: keepAlive,
		tlsConfig: cfg,
	}
}

func (c *Client) Connect(ctx context.Context, addr string) error {
	conn, e := quic.DialAddr(ctx, addr, c.tlsConfig, c.quicConfig)
	if e != nil {
		return e
	}

	stream, e := conn.OpenStreamSync(ctx)
	if e != nil {
		_ = conn.CloseWithError(ConnectionCloseNormal, "open stream failed")
		return e
	}

	cp := packet.ConnectPacket{
		ProtocolLevel:   byte(packet.MqttVersion311),
		ClientIdentifer: c.clientID,
		CleanSession:    true,
		KeepAlive:       uint16(c.keepAlive / time.Second),
	}

	var data []byte
	e = cp.Encode(&data)
	if e != nil {
		_ = conn.CloseWithError(ConnectionCloseNormal, "encode failed")
		return e
	}

	_, e = stream.Write(data)
	if e != nil {
		_ = conn.CloseWithError(ConnectionCloseNormal, "write failed")
		return e
	}

	c.conn = conn
	c.stream = stream
	return nil
}
