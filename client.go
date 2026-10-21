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

func New(clientID string, keepAlive time.Duration, tlsConfig *tls.Config) *Client {
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
		_ = conn.CloseWithError(0, "open stream failed")
		return e
	}

	var cp packet.ConnectPacket
	cp.ProtocolLevel = byte(packet.MqttVersion311)
	cp.ClientIdentifer = c.clientID
	cp.CleanSession = true
	cp.KeepAlive = uint16(c.keepAlive / time.Second)

	data, e := cp.Encode()
	if e != nil {
		return e
	}

	_, e = stream.Write(data)
	if e != nil {
		return e
	}

}

/*

package mqttOQ

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
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

func New(clientID string, keepAlive time.Duration, tlsConfig tls.Config) *Client {

	tlsConfig = *tlsConfig.Clone()
	tlsConfig.NextProtos = []string{"mqtt"}

	var newClient Client
	newClient.clientID = clientID
	newClient.keepAlive = keepAlive
	newClient.tlsConfig = &tlsConfig
	return &newClient
}

func (c *Client) Connect(ctx context.Context, addr string) error {
	conn, err := quic.DialAddr(ctx, addr, c.tlsConfig, c.quicConfig)
	if err != nil {
		return fmt.Errorf("quic dial: %w", err)
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(1, "open stream failed")
		return fmt.Errorf("open stream: %w", err)
	}

	// Close the whole connection on any failure below; keep it open on success.
	ok := false
	defer func() {
		if !ok {
			_ = conn.CloseWithError(1, "connect failed")
		}
	}()

	cp := packet.ConnectPacket{
		ProtocolLevel:   byte(packet.MqttVersion311),
		CleanSession:    true,
		KeepAlive:       uint16(c.keepAlive / time.Second),
		ClientIdentifer: c.clientID,
	}
	data, err := cp.Encode()
	if err != nil {
		return err
	}
	// The server only sees the stream after data is written.
	if _, err := stream.Write(data); err != nil {
		return fmt.Errorf("write CONNECT: %w", err)
	}

	// Bound the CONNACK wait by the context deadline, if any.
	if dl, hasDeadline := ctx.Deadline(); hasDeadline {
		_ = stream.SetReadDeadline(dl)
	}
	// CONNACK is always 4 bytes: 20 02 <session present> <return code>
	resp := make([]byte, 4)
	if _, err := io.ReadFull(stream, resp); err != nil {
		return fmt.Errorf("read CONNACK: %w", err)
	}
	_ = stream.SetReadDeadline(time.Time{})

	if resp[0] != 0x20 || resp[1] != 0x02 {
		return fmt.Errorf("unexpected reply: % X", resp)
	}
	if err := packet.ConnectReturnCode(resp[3]).Err(); err != nil {
		return err // refused by the broker
	}

	c.conn, c.stream = conn, stream
	ok = true
	return nil
}

*/
