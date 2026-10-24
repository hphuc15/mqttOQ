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

type Client struct {
	cfgs   ClientConfig
	conn   *quic.Conn
	stream *quic.Stream
	state  *atomicState
}

const ClientDafaultClientID = "mqttOQ-client-%d"
const ClientDafaultKeepAlive = uint16(120) // seconds
const ClientDafaulConnectTimeout = 10 * time.Second

func NewClient(cfgs ClientConfig) *Client {
	if cfgs.Credentials.ClientID == "" {
		cfgs.Credentials.ClientID = fmt.Sprintf(ClientDafaultClientID, time.Now().UnixNano())
	}

	if cfgs.Session.KeepAlive == 0 {
		cfgs.Session.KeepAlive = ClientDafaultKeepAlive
	}

	if cfgs.Network.ConnectTimeout == 0 {
		cfgs.Network.ConnectTimeout = ClientDafaulConnectTimeout
	}

	return &Client{
		cfgs: cfgs,
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

// 	e := c.

// 	return nil
// }

// connectTransport performs the QUIC dial and sends the MQTT CONNECT packet.
func (c *Client) connectTransport(ctx context.Context) error {
	tlsConfig := c.cfgs.TLSConfig
	if tlsConfig == nil {
		tlsConfig = transport.NewClientTLSConfig(true, "mqtt")
	}

	conn, e := transport.Dial(ctx, c.cfgs.Broker.Address, tlsConfig, c.cfgs.QUICConfig)
	if e != nil {
		return fmt.Errorf("QUIC dial error: %w", e)
	}

	stream, e := conn.OpenStreamSync(ctx)
	if e != nil {
		conn.Close()
		return fmt.Errorf("failed to open QUIC stream: %w", e)
	}

	c.conn = conn
	c.stream = stream

	cPacket := &packet.ConnectPacket{
		ClientIdentifer: c.cfgs.Credentials.ClientID,
		CleanSession:    c.cfgs.Session.CleanSession,
		KeepAlive:       c.cfgs.Session.KeepAlive,
		UserName:        c.cfgs.Credentials.UserName,
		Password:        []byte(c.cfgs.Credentials.Password),
		WillFlag:        c.cfgs.Session.WillTopic != "",
		WillMessage:     c.cfgs.Session.WillMessage,
		WillQos:         c.cfgs.Session.WillQoS,
		WillRetain:      c.cfgs.Session.WillRetain,
	}

	e := cPacket.Encode()
}
