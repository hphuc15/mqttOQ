package client

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hphuc15/mqttOQ/packet"
)

var (
	ErrInvalidQoS   = errors.New("invalid QoS level: must be 0, 1, or 2")
	ErrInvalidTopic = errors.New("invalid topic name: cannot be empty or contain wildcards (+, #)")
)

// nextPacketID generates the next available non-zero 16-bit packet identifier.
// Per MQTT 3.1.1 section 2.3.1, a packet ID MUST NOT be reused while there
// is an unacknowledged packet with the same ID outstanding. This includes both
// IDs that have a registered ack waiter (current in-progress calls) and IDs
// that are tracked by the inflightManager (messages surviving a reconnect).
func (c *Client) nextPacketID() uint16 {
	c.pidMu.Lock()
	defer c.pidMu.Unlock()

	for {
		c.lastPacketID++
		if c.lastPacketID == 0 {
			c.lastPacketID = 1
		}

		pid := c.lastPacketID

		c.ackMu.RLock()
		_, inAckWaiters := c.ackWaiters[pid]
		c.ackMu.RUnlock()

		if inAckWaiters {
			continue
		}

		if c.inflight != nil && c.inflight.contains(pid) {
			continue
		}

		return pid
	}
}

// Publish sends an MQTT message to the specified topic with the given QoS and retain flag.
func (c *Client) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}

	if qos > 2 {
		return ErrInvalidQoS
	}

	if topic == "" || strings.ContainsAny(topic, "+#") {
		return ErrInvalidTopic
	}

	switch packet.QualityOfService(qos) {
	case packet.QoS0:
		return c.publishQoS0(topic, retain, payload)
	case packet.QoS1:
		return c.publishQoS1(ctx, topic, retain, payload)
	case packet.QoS2:
		return c.publishQoS2(ctx, topic, retain, payload)
	default:
		return ErrInvalidQoS
	}
}

// PublishMessage publishes a Message struct instance.
func (c *Client) PublishMessage(ctx context.Context, msg Message) error {
	return c.Publish(ctx, msg.Topic, msg.QoS, msg.Retained, msg.Payload)
}

func (c *Client) publishQoS0(topic string, retain bool, payload []byte) error {
	pkt := &packet.PublishPacket{
		Config: packet.PublishOthersConfig{
			QosLevel: packet.QoS0,
			Retain:   retain,
		},
		TopicName: topic,
		Payload:   payload,
	}
	return c.sendDataPacket(pkt)
}

func (c *Client) publishQoS1(ctx context.Context, topic string, retain bool, payload []byte) error {
	pid := c.nextPacketID()
	ackCh := c.registerAckWaiter(pid)
	defer c.unregisterAckWaiter(pid)

	pkt := &packet.PublishPacket{
		Config: packet.PublishOthersConfig{
			QosLevel: packet.QoS1,
			Retain:   retain,
		},
		TopicName:        topic,
		PacketIdentifier: pid,
		Payload:          payload,
	}

	if c.inflight != nil {
		c.inflight.add(pid, pkt, inflightWaitPuback)
		defer c.inflight.remove(pid)
	}

	if err := c.sendDataPacket(pkt); err != nil {
		if !c.config.Network.AutoReconnect {
			return err
		}
	}

	select {
	case resp := <-ackCh:
		if _, ok := resp.(*packet.PubackPacket); !ok {
			return fmt.Errorf("expected PUBACK for packet ID %d, got %s", pid, resp.Type())
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeCh:
		if !c.config.Network.AutoReconnect {
			return ErrConnectionClosed
		}
		select {
		case resp := <-ackCh:
			if _, ok := resp.(*packet.PubackPacket); !ok {
				return fmt.Errorf("expected PUBACK for packet ID %d, got %s", pid, resp.Type())
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return ErrConnectionClosed
		}
	case <-c.stopCh:
		return ErrConnectionClosed
	}
}

func (c *Client) publishQoS2(ctx context.Context, topic string, retain bool, payload []byte) error {
	pid := c.nextPacketID()
	ackCh := c.registerAckWaiter(pid)
	defer c.unregisterAckWaiter(pid)

	pkt := &packet.PublishPacket{
		Config: packet.PublishOthersConfig{
			QosLevel: packet.QoS2,
			Retain:   retain,
		},
		TopicName:        topic,
		PacketIdentifier: pid,
		Payload:          payload,
	}

	if c.inflight != nil {
		c.inflight.add(pid, pkt, inflightWaitPubrec)
		defer c.inflight.remove(pid)
	}

	// Step 1: Send PUBLISH
	if err := c.sendDataPacket(pkt); err != nil {
		if !c.config.Network.AutoReconnect {
			return err
		}
	}

	// Step 2: Await PUBREC
	select {
	case resp := <-ackCh:
		if _, ok := resp.(*packet.PubrecPacket); !ok {
			return fmt.Errorf("expected PUBREC for packet ID %d, got %s", pid, resp.Type())
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeCh:
		if !c.config.Network.AutoReconnect {
			return ErrConnectionClosed
		}
		select {
		case resp := <-ackCh:
			if _, ok := resp.(*packet.PubrecPacket); !ok {
				return fmt.Errorf("expected PUBREC for packet ID %d, got %s", pid, resp.Type())
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return ErrConnectionClosed
		}
	case <-c.stopCh:
		return ErrConnectionClosed
	}

	// Step 3: Send PUBREL
	if c.inflight != nil {
		c.inflight.updateState(pid, inflightWaitPubcomp)
	}
	pubrel := &packet.PubrelPacket{PacketIdentifier: pid}
	if err := c.sendDataPacket(pubrel); err != nil {
		if !c.config.Network.AutoReconnect {
			return err
		}
	}

	// Step 4: Await PUBCOMP
	select {
	case resp := <-ackCh:
		if _, ok := resp.(*packet.PubcompPacket); !ok {
			return fmt.Errorf("expected PUBCOMP for packet ID %d, got %s", pid, resp.Type())
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeCh:
		if !c.config.Network.AutoReconnect {
			return ErrConnectionClosed
		}
		select {
		case resp := <-ackCh:
			if _, ok := resp.(*packet.PubcompPacket); !ok {
				return fmt.Errorf("expected PUBCOMP for packet ID %d, got %s", pid, resp.Type())
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return ErrConnectionClosed
		}
	case <-c.stopCh:
		return ErrConnectionClosed
	}
}
