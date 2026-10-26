package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
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
	ConnectTimeout       time.Duration
	AutoReconnect        bool
	ReconnectInterval    time.Duration
	MaxReconnectInterval time.Duration
	Enable0RTT           bool
	EnableMultiStream    bool
}

type ClientConfig struct {
	Broker               ClientBroker
	Credentials          ClientCredentials
	Session              ClientSession
	Network              ClientNetwork
	TLSConfig            *tls.Config
	QUICConfig           *quic.Config
	DefaultMessageHandler MessageHandler
}

// streamRW is the minimal io interface required by sendPacket and readLoop.
// Using an interface instead of *quic.Stream allows stream injection in tests.
type streamRW interface {
	Read(p []byte) (n int, err error)
	Write(p []byte) (n int, err error)
	Close() error
}

type Client struct {
	config     ClientConfig
	state      *atomicState
	writeMu    sync.Mutex
	conn       *quic.Conn
	stream     streamRW // Control stream (primary stream)
	dispatcher *messageDispatcher
	closeCh    chan struct{}
	closeOnce  sync.Once
	stopCh     chan struct{}
	stopOnce   sync.Once
	wg         sync.WaitGroup

	ackMu        sync.RWMutex
	ackWaiters   map[uint16]chan packet.ControlPacket
	pingRespCh   chan struct{}
	pidMu        sync.Mutex
	lastPacketID uint16

	inflight      *inflightManager
	subMu         sync.RWMutex
	subscriptions map[string]byte

	reconnectMu     sync.Mutex
	isReconnecting  bool
	dialTransportFn func(ctx context.Context, early bool) (streamRW, error)

	opener     streamOpener
	dataMu     sync.Mutex
	dataStream streamRW
}

const (
	ClientDefaultClientID       = "mqttOQ-client-%d"
	ClientDefaultKeepAlive      = uint16(120) // seconds
	ClientDefaultConnectTimeout = 10 * time.Second

	// Backward-compatible aliases for legacy spellings
	ClientDafaultClientID       = ClientDefaultClientID
	ClientDafaultKeepAlive      = ClientDefaultKeepAlive
	ClientDafaulConnectTimeout  = ClientDefaultConnectTimeout
)

func NewClient(config ClientConfig) *Client {
	if config.Credentials.ClientID == "" {
		config.Credentials.ClientID = fmt.Sprintf(ClientDefaultClientID, time.Now().UnixNano())
	}

	if config.Session.KeepAlive == 0 {
		config.Session.KeepAlive = ClientDefaultKeepAlive
	}

	if config.Network.ConnectTimeout == 0 {
		config.Network.ConnectTimeout = ClientDefaultConnectTimeout
	}

	if config.Network.ReconnectInterval == 0 {
		config.Network.ReconnectInterval = 1 * time.Second
	}

	if config.Network.MaxReconnectInterval == 0 {
		config.Network.MaxReconnectInterval = 30 * time.Second
	}

	if config.TLSConfig == nil {
		config.TLSConfig = transport.NewClientTLSConfig(true, "mqtt")
	}

	return &Client{
		config:        config,
		state:         newAtomicState(StateDisconnected),
		dispatcher:    newDispatcher(config.DefaultMessageHandler),
		closeCh:       make(chan struct{}),
		stopCh:        make(chan struct{}),
		ackWaiters:    make(map[uint16]chan packet.ControlPacket),
		inflight:      newInflightManager(),
		subscriptions: make(map[string]byte),
	}
}

// State returns the current connection lifecycle state.
func (c *Client) State() ConnectionState {
	if c.state == nil {
		return StateDisconnected
	}
	return c.state.Load()
}

// IsConnected returns true if the client is currently connected.
func (c *Client) IsConnected() bool {
	return c.State() == StateConnected
}

// Config returns the configuration of the client.
func (c *Client) Config() ClientConfig {
	return c.config
}

// LocalAddr returns the local network address of the underlying QUIC connection.
func (c *Client) LocalAddr() net.Addr {
	if c.conn != nil {
		return c.conn.LocalAddr()
	}
	return nil
}

// RemoteAddr returns the remote network address of the underlying QUIC connection.
func (c *Client) RemoteAddr() net.Addr {
	if c.conn != nil {
		return c.conn.RemoteAddr()
	}
	return nil
}

// Conn returns the underlying QUIC connection (or nil if disconnected).
func (c *Client) Conn() *quic.Conn {
	return c.conn
}

// MigratePath migrates the client's connection to a new network path using the provided transport.
func (c *Client) MigratePath(ctx context.Context, newTransport *quic.Transport) error {
	if c.conn == nil {
		return ErrNotConnected
	}
	path, err := c.conn.AddPath(newTransport)
	if err != nil {
		return fmt.Errorf("failed to add path: %w", err)
	}
	if err := path.Probe(ctx); err != nil {
		_ = path.Close()
		return fmt.Errorf("failed to probe path: %w", err)
	}
	if err := path.Switch(); err != nil {
		_ = path.Close()
		return fmt.Errorf("failed to switch path: %w", err)
	}
	return nil
}

// RegisterRoute registers a message handler for a specific topic filter.
func (c *Client) RegisterRoute(topicFilter string, handler MessageHandler) {
	c.dispatcher.register(topicFilter, handler)
}

// UnregisterRoute unregisters a message handler for a topic filter.
func (c *Client) UnregisterRoute(topicFilter string) {
	c.dispatcher.unregister(topicFilter)
}

// SetDefaultMessageHandler sets or replaces the default fallback message handler.
func (c *Client) SetDefaultMessageHandler(handler MessageHandler) {
	c.dispatcher.setDefaultHandler(handler)
}

var (
	ErrAlreadyConnected   = errors.New("client is already connected")
	ErrNotConnected       = errors.New("client is not connected")
	ErrConnectionClosed   = errors.New("connection closed")
	ErrPingTimeout        = errors.New("keep-alive ping timeout")
	ErrSubscriptionFailed = errors.New("broker rejected subscription")
)

func (c *Client) Connect(ctx context.Context) error {
	if !c.state.CompareAndSwap(StateDisconnected, StateConnecting) {
		return ErrAlreadyConnected
	}

	e := c.connectTransport(ctx)
	if e != nil {
		c.state.Set(StateDisconnected)
		return e
	}

	c.startReaderLoop()
	c.startKeepAlive()
	if c.config.Network.EnableMultiStream {
		c.startAcceptStreamLoop()
	}
	c.state.Set(StateConnected)

	return nil
}

// connectTransport performs the QUIC dial and sends the MQTT CONNECT packet.
func (c *Client) connectTransport(ctx context.Context) error {
	tlsConfig := c.config.TLSConfig
	if tlsConfig == nil {
		tlsConfig = transport.NewClientTLSConfig(true, "mqtt")
	}

	var conn *quic.Conn
	var stream streamRW
	var e error

	if c.dialTransportFn != nil {
		stream, e = c.dialTransportFn(ctx, false)
		if e != nil {
			return e
		}
	} else {
		conn, e = transport.Dial(ctx, c.config.Broker.Address, tlsConfig, c.config.QUICConfig)
		if e != nil {
			return fmt.Errorf("QUIC dial error: %w", e)
		}

		s, streamErr := conn.OpenStreamSync(ctx)
		if streamErr != nil {
			_ = conn.CloseWithError(0, "")
			return fmt.Errorf("failed to open QUIC stream: %w", streamErr)
		}
		stream = s
		c.conn = conn
		c.opener = &quicStreamOpener{conn: conn}
	}

	c.stream = stream

	cPacket := &packet.ConnectPacket{
		ProtocolName:     "MQTT",
		ProtocolLevel:    4,
		ClientIdentifier: c.config.Credentials.ClientID,
		CleanSession:     c.config.Session.CleanSession,
		KeepAlive:        c.config.Session.KeepAlive,
		UserNameFlag:     c.config.Credentials.UserName != "",
		UserName:         c.config.Credentials.UserName,
		PasswordFlag:     c.config.Credentials.Password != "",
		Password:         []byte(c.config.Credentials.Password),
		WillFlag:         c.config.Session.WillTopic != "",
		WillTopic:        c.config.Session.WillTopic,
		WillMessage:      c.config.Session.WillMessage,
		WillQos:          c.config.Session.WillQoS,
		WillRetain:       c.config.Session.WillRetain,
	}

	e = cPacket.Encode(stream)
	if e != nil {
		c.cleanupTransport()
		return fmt.Errorf("encode CONNECT error: %w", e)
	}

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

func (c *Client) sendPacket(pkt packet.ControlPacket) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.stream == nil {
		return ErrConnectionClosed
	}

	return pkt.Encode(c.stream)
}

func (c *Client) registerAckWaiter(packetID uint16) chan packet.ControlPacket {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()

	ch := make(chan packet.ControlPacket, 1)
	c.ackWaiters[packetID] = ch
	return ch
}

func (c *Client) unregisterAckWaiter(packetID uint16) {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()

	delete(c.ackWaiters, packetID)
}

func (c *Client) notifyAckWaiter(packetID uint16, pkt packet.ControlPacket) {
	c.ackMu.RLock()
	ch, ok := c.ackWaiters[packetID]
	c.ackMu.RUnlock()

	if ok && ch != nil {
		select {
		case ch <- pkt:
		default:
		}
	}
}

func (c *Client) startReaderLoop() {
	c.closeOnce = sync.Once{}
	c.closeCh = make(chan struct{})
	c.wg.Add(1)
	go c.readLoop()
}

func (c *Client) readLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.closeCh:
			return
		default:
		}

		if c.stream == nil {
			return
		}

		pkt, err := packet.ReadPacket(c.stream)
		if err != nil {
			select {
			case <-c.closeCh:
				return
			default:
				c.handleUnexpectedDisconnect()
				return
			}
		}

		c.handleIncomingPacket(pkt)
	}
}

func (c *Client) handleIncomingPacket(pkt packet.ControlPacket) {
	switch p := pkt.(type) {
	case *packet.PublishPacket:
		msg := Message{
			Topic:     p.TopicName,
			Payload:   p.Payload,
			QoS:       byte(p.Config.QosLevel),
			Retained:  p.Config.Retain,
			Duplicate: p.Config.DupFlag,
		}

		// Acknowledge QoS 1 and QoS 2 inbound packets
		if p.Config.QosLevel == packet.QoS1 {
			_ = c.sendPacket(&packet.PubackPacket{PacketIdentifier: p.PacketIdentifier})
		} else if p.Config.QosLevel == packet.QoS2 {
			_ = c.sendPacket(&packet.PubrecPacket{PacketIdentifier: p.PacketIdentifier})
		}

		// Dispatch message asynchronously so readLoop is never blocked
		go c.dispatcher.dispatch(c, msg)

	case *packet.PubackPacket:
		if c.inflight != nil {
			c.inflight.remove(p.PacketIdentifier)
		}
		c.notifyAckWaiter(p.PacketIdentifier, p)

	case *packet.PubrecPacket:
		if c.inflight != nil {
			c.inflight.updateState(p.PacketIdentifier, inflightWaitPubcomp)
		}
		c.notifyAckWaiter(p.PacketIdentifier, p)

	case *packet.PubrelPacket:
		// QoS 2 inbound step 2: reply with PUBCOMP
		_ = c.sendPacket(&packet.PubcompPacket{PacketIdentifier: p.PacketIdentifier})

	case *packet.PubcompPacket:
		if c.inflight != nil {
			c.inflight.remove(p.PacketIdentifier)
		}
		c.notifyAckWaiter(p.PacketIdentifier, p)

	case *packet.SubackPacket:
		c.notifyAckWaiter(p.PacketIdentifier, p)

	case *packet.UnsubackPacket:
		c.notifyAckWaiter(p.PacketIdentifier, p)

	case *packet.PingrespPacket:
		if c.pingRespCh != nil {
			select {
			case c.pingRespCh <- struct{}{}:
			default:
			}
		}
	}
}

func (c *Client) handleUnexpectedDisconnect() {
	c.cleanupTransport()
	if c.state.Load() == StateClosed {
		return
	}
	c.state.Set(StateDisconnected)

	if c.config.Network.AutoReconnect {
		c.triggerReconnect()
	}
}

func (c *Client) cleanupTransport() {
	c.closeOnce.Do(func() {
		close(c.closeCh)
	})

	c.dataMu.Lock()
	if c.dataStream != nil {
		_ = c.dataStream.Close()
		c.dataStream = nil
	}
	c.dataMu.Unlock()

	if c.stream != nil {
		_ = c.stream.Close()
		c.stream = nil
	}
	if c.conn != nil {
		_ = c.conn.CloseWithError(0, "client disconnected")
		c.conn = nil
	}
}
