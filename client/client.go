package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/hphuc15/mqttOQ/packet"
	"github.com/hphuc15/mqttOQ/transport"
	"github.com/quic-go/quic-go"
)

type ClientBroker struct {
	Address string
}

type ClientCredentials struct {
	ClientID string
	UserName string
	Password string
}

type ClientSession struct {
	ProtocolVersion []byte
	CleanSession    bool
	KeepAlive       uint16
	WillTopic       string
	WillMessage     []byte
	WillQoS         byte
	WillRetain      bool
}

type ClientNetwork struct {
	ConnectTimeout    time.Duration
	AutoReconnect     bool
	ReconnectInterval time.Duration
}

type ClientConfig struct {
	Broker      ClientBroker
	Credentials ClientCredentials
	Session     ClientSession
	Network     ClientNetwork
	TLSConfig   *tls.Config
	QUICConfig  *quic.Config
}

type ClientConfig2 struct {
	ClientID string
	Conn     quic.Conn
	// Conn     net.Conn
	// Session  session.SessionManager

}

type Client struct {
	config ClientConfig
	conn   *quic.Conn
	stream *quic.Stream
	state  *atomicState
}

const ClientDafaultClientID = "mqttOQ-client-%d"
const ClientDafaultKeepAlive = uint16(120) // seconds
const ClientDafaulConnectTimeout = 10 * time.Second

func NewClient(config ClientConfig) *Client {
	if config.Credentials.ClientID == "" {
		config.Credentials.ClientID = fmt.Sprintf(ClientDafaultClientID, time.Now().UnixNano())
	}

	if config.Session.KeepAlive == 0 {
		config.Session.KeepAlive = ClientDafaultKeepAlive
	}

	if config.Network.ConnectTimeout == 0 {
		config.Network.ConnectTimeout = ClientDafaulConnectTimeout
	}

	return &Client{
		config: config,
		// ...
	}
}

var (
	ErrAlreadyConnected   = errors.New("client is already connected")
	ErrNotConnected       = errors.New("client is not connected")
	ErrConnectionClosed   = errors.New("connection closed")
	ErrPingTimeout        = errors.New("keep-alive ping timeout")
	ErrSubscriptionFailed = errors.New("broker rejected subscription")
)

// func (c *Client) Connect(ctx context.Context) error {
// 	if !c.state.CompareAndSwap(StateDisconnected, StateConnecting) {
// 		return ErrAlreadyConnected
// 	}

// 	e := c.connectTransport(ctx)
// 	if e != nil {
// 		c.state.Set(StateDisconnected)
// 		return e
// 	}

// 	c.state.Set(StateConnected)

// 	c.clo

// 	return nil
// }

// connectTransport performs the QUIC dial and sends the MQTT CONNECT packet.
func (c *Client) connectTransport(ctx context.Context) error {
	tlsConfig := c.config.TLSConfig
	if tlsConfig == nil {
		tlsConfig = transport.NewClientTLSConfig(true, "mqtt")
	}

	conn, e := transport.Dial(ctx, c.config.Broker.Address, tlsConfig, c.config.QUICConfig)
	if e != nil {
		return fmt.Errorf("QUIC dial error: %w", e)
	}

	stream, e := conn.OpenStreamSync(ctx)
	if e != nil {
		conn.CloseWithError(0, "")
		return fmt.Errorf("failed to open QUIC stream: %w", e)
	}

	c.conn = conn
	c.stream = stream

	cPacket := &packet.ConnectPacket{
		ClientIdentifier: c.config.Credentials.ClientID,
		CleanSession:     c.config.Session.CleanSession,
		KeepAlive:        c.config.Session.KeepAlive,
		UserName:         c.config.Credentials.UserName,
		Password:         []byte(c.config.Credentials.Password),
		WillFlag:         c.config.Session.WillTopic != "",
		WillMessage:      c.config.Session.WillMessage,
		WillQos:          c.config.Session.WillQoS,
		WillRetain:       c.config.Session.WillRetain,
	}

	e = cPacket.Encode(stream)
	if e != nil {
		c.cleanupTransport()
		return fmt.Errorf("encode CONNECT error: %w", e)
	}

	// Receive CONNACK response will be implement here:
	// ......
	// Read CONNACK synchronously with timeout
	respPacket, e := packet.ReadPacket(stream)
	if e != nil {
		c.cleanupTransport()
		return fmt.Errorf("read CONNACK error: %w", e)
	}

	connack, ok := respPacket.(*packet.ConnackPacket)
	if !ok {
		c.cleanupTransport()
		return fmt.Errorf("expected CONNACK packet, got %s", respPacket.Type())
	}

	if connack.ReturnCode != packet.AcceptedConnection {
		c.cleanupTransport()
		return fmt.Errorf("connection refused: %s", connack.ReturnCode)
	}

	return nil
}

func (c *Client) cleanupTransport() {
	if c.stream != nil {
		_ = c.stream.Close()
		c.stream = nil
	}
	if c.conn != nil {
		_ = c.conn.CloseWithError(0, "")
		c.conn = nil
	}
}
