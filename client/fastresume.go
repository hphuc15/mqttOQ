package client

import (
	"context"
	"fmt"

	"github.com/hphuc15/mqttOQ/packet"
	"github.com/hphuc15/mqttOQ/transport"
	"github.com/quic-go/quic-go"
)

// connectTransportEarly initiates a 0-RTT QUIC connection and sends the MQTT
// CONNECT packet immediately within the 0-RTT early data window.
func (c *Client) connectTransportEarly(ctx context.Context) error {
	tlsConfig := c.config.TLSConfig
	if tlsConfig == nil {
		tlsConfig = transport.NewClientTLSConfig(true, "mqtt")
	}

	var conn *quic.Conn
	var stream streamRW
	var err error

	if c.dialTransportFn != nil {
		stream, err = c.dialTransportFn(ctx, true)
		if err != nil {
			return c.connectTransport(ctx)
		}
	} else {
		conn, err = transport.DialEarly(ctx, c.config.Broker.Address, tlsConfig, c.config.QUICConfig)
		if err != nil {
			// If 0-RTT dial fails, fallback to standard 1-RTT dial
			return c.connectTransport(ctx)
		}

		s, streamErr := conn.OpenStreamSync(ctx)
		if streamErr != nil {
			_ = conn.CloseWithError(0, "")
			// Fallback to standard 1-RTT dial
			return c.connectTransport(ctx)
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

	err = cPacket.Encode(stream)
	if err != nil {
		c.cleanupTransport()
		return fmt.Errorf("encode 0-RTT CONNECT error: %w", err)
	}

	// Read CONNACK synchronously with timeout
	respPacket, err := packet.ReadPacket(stream)
	if err != nil {
		c.cleanupTransport()
		return fmt.Errorf("read CONNACK error: %w", err)
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
