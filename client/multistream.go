package client

import (
	"context"

	"github.com/hphuc15/mqttOQ/packet"
	"github.com/quic-go/quic-go"
)

// streamOpener defines an interface to open and accept QUIC streams.
type streamOpener interface {
	OpenStream(ctx context.Context) (streamRW, error)
	AcceptStream(ctx context.Context) (streamRW, error)
}

// quicStreamOpener adapts a concrete *quic.Conn to the streamOpener interface.
type quicStreamOpener struct {
	conn *quic.Conn
}

func (q *quicStreamOpener) OpenStream(ctx context.Context) (streamRW, error) {
	if q.conn == nil {
		return nil, ErrConnectionClosed
	}
	return q.conn.OpenStreamSync(ctx)
}

func (q *quicStreamOpener) AcceptStream(ctx context.Context) (streamRW, error) {
	if q.conn == nil {
		return nil, ErrConnectionClosed
	}
	return q.conn.AcceptStream(ctx)
}

// getDataStream returns an existing or freshly opened data stream for publishing.
func (c *Client) getDataStream(ctx context.Context) (streamRW, error) {
	c.dataMu.Lock()
	defer c.dataMu.Unlock()

	if c.dataStream != nil {
		return c.dataStream, nil
	}

	if c.opener == nil {
		return nil, ErrConnectionClosed
	}

	stream, err := c.opener.OpenStream(ctx)
	if err != nil {
		return nil, err
	}

	c.dataStream = stream
	c.startDataStreamReader(stream)
	return c.dataStream, nil
}

// startDataStreamReader starts a goroutine to read inbound packets (such as PUBACK/PUBREC) on the data stream.
func (c *Client) startDataStreamReader(stream streamRW) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.closeCh:
				return
			default:
			}

			pkt, err := packet.ReadPacket(stream)
			if err != nil {
				return
			}

			c.handleIncomingPacket(pkt)
		}
	}()
}

// startAcceptStreamLoop starts a background loop to accept incoming data streams from the broker.
func (c *Client) startAcceptStreamLoop() {
	if c.opener == nil {
		return
	}

	c.wg.Add(1)
	go c.acceptStreamLoop()
}

// acceptStreamLoop accepts incoming QUIC streams initiated by the broker and reads messages from them.
func (c *Client) acceptStreamLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.closeCh:
			return
		default:
		}

		if c.opener == nil {
			return
		}

		stream, err := c.opener.AcceptStream(context.Background())
		if err != nil {
			return
		}

		c.wg.Add(1)
		go func(s streamRW) {
			defer c.wg.Done()
			defer s.Close()

			for {
				select {
				case <-c.closeCh:
					return
				default:
				}

				pkt, err := packet.ReadPacket(s)
				if err != nil {
					return
				}

				// If it's an inbound publish, respond on the same data stream
				if pub, ok := pkt.(*packet.PublishPacket); ok {
					msg := Message{
						Topic:     pub.TopicName,
						Payload:   pub.Payload,
						QoS:       byte(pub.Config.QosLevel),
						Retained:  pub.Config.Retain,
						Duplicate: pub.Config.DupFlag,
					}

					if pub.Config.QosLevel == packet.QoS1 {
						_ = (&packet.PubackPacket{PacketIdentifier: pub.PacketIdentifier}).Encode(s)
					} else if pub.Config.QosLevel == packet.QoS2 {
						_ = (&packet.PubrecPacket{PacketIdentifier: pub.PacketIdentifier}).Encode(s)
					}

					go c.dispatcher.dispatch(c, msg)
				} else {
					c.handleIncomingPacket(pkt)
				}
			}
		}(stream)
	}
}

// sendDataPacket sends a publish or publish-ack packet on the data stream when multi-stream is enabled,
// or falls back to the primary control stream.
func (c *Client) sendDataPacket(pkt packet.ControlPacket) error {
	if !c.config.Network.EnableMultiStream {
		return c.sendPacket(pkt)
	}

	stream, err := c.getDataStream(context.Background())
	if err != nil {
		return c.sendPacket(pkt)
	}

	c.dataMu.Lock()
	defer c.dataMu.Unlock()
	return pkt.Encode(stream)
}

// sendControlPacket sends control packets (CONNECT, SUBSCRIBE, UNSUBSCRIBE, PING, DISCONNECT)
// strictly on the control stream.
func (c *Client) sendControlPacket(pkt packet.ControlPacket) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.stream == nil {
		return ErrConnectionClosed
	}

	return pkt.Encode(c.stream)
}
