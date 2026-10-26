package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/hphuc15/mqttOQ/packet"
)

var (
	ErrNoSubscriptions     = errors.New("subscribe requires at least one topic filter")
	ErrNoUnsubscribeTopics = errors.New("unsubscribe requires at least one topic filter")
)

// Subscription represents a topic filter and requested QoS level.
type Subscription struct {
	Topic string
	QoS   byte
}

// Subscribe subscribes to a single topic with the requested QoS and registers a message handler.
func (c *Client) Subscribe(ctx context.Context, topic string, qos byte, handler MessageHandler) error {
	if topic == "" {
		return ErrInvalidTopic
	}
	return c.SubscribeMultiple(ctx, []Subscription{{Topic: topic, QoS: qos}}, handler)
}

// SubscribeMultiple subscribes to multiple topic filters with requested QoS levels and registers a message handler.
func (c *Client) SubscribeMultiple(ctx context.Context, subscriptions []Subscription, handler MessageHandler) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}

	if len(subscriptions) == 0 {
		return ErrNoSubscriptions
	}

	pktSubs := make([]packet.TopicSubscription, 0, len(subscriptions))
	for _, sub := range subscriptions {
		if sub.Topic == "" {
			return ErrInvalidTopic
		}
		if sub.QoS > 2 {
			return ErrInvalidQoS
		}
		pktSubs = append(pktSubs, packet.TopicSubscription{
			TopicFilter:  sub.Topic,
			RequestedQos: packet.QualityOfService(sub.QoS),
		})
	}

	pid := c.nextPacketID()
	ackCh := c.registerAckWaiter(pid)
	defer c.unregisterAckWaiter(pid)

	pkt := &packet.SubscribePacket{
		PacketIdentifier: pid,
		Subscriptions:    pktSubs,
	}

	if err := c.sendPacket(pkt); err != nil {
		return err
	}

	select {
	case resp := <-ackCh:
		suback, ok := resp.(*packet.SubackPacket)
		if !ok {
			return fmt.Errorf("expected SUBACK for packet ID %d, got %s", pid, resp.Type())
		}

		if len(suback.ReturnCodes) != len(subscriptions) {
			return fmt.Errorf("SUBACK return codes count mismatch: expected %d, got %d", len(subscriptions), len(suback.ReturnCodes))
		}

		for i, code := range suback.ReturnCodes {
			if code == byte(packet.FailureSuback) {
				return fmt.Errorf("%w for topic %q", ErrSubscriptionFailed, subscriptions[i].Topic)
			}
		}

		// Register handlers upon successful subscription and track active subscriptions
		c.subMu.Lock()
		for _, sub := range subscriptions {
			if handler != nil {
				c.RegisterRoute(sub.Topic, handler)
			}
			if c.subscriptions != nil {
				c.subscriptions[sub.Topic] = sub.QoS
			}
		}
		c.subMu.Unlock()

		return nil

	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeCh:
		return ErrConnectionClosed
	}
}

// Unsubscribe unsubscribes from one or more topic filters and removes their registered handlers.
func (c *Client) Unsubscribe(ctx context.Context, topics ...string) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}

	if len(topics) == 0 {
		return ErrNoUnsubscribeTopics
	}

	for _, t := range topics {
		if t == "" {
			return ErrInvalidTopic
		}
	}

	pid := c.nextPacketID()
	ackCh := c.registerAckWaiter(pid)
	defer c.unregisterAckWaiter(pid)

	pkt := &packet.UnsubscribePacket{
		PacketIdentifier: pid,
		TopicFilters:     topics,
	}

	if err := c.sendPacket(pkt); err != nil {
		return err
	}

	select {
	case resp := <-ackCh:
		if _, ok := resp.(*packet.UnsubackPacket); !ok {
			return fmt.Errorf("expected UNSUBACK for packet ID %d, got %s", pid, resp.Type())
		}

		// Unregister route handlers for unsubscribed topics and remove from active subscriptions
		c.subMu.Lock()
		for _, t := range topics {
			c.UnregisterRoute(t)
			if c.subscriptions != nil {
				delete(c.subscriptions, t)
			}
		}
		c.subMu.Unlock()

		return nil

	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeCh:
		return ErrConnectionClosed
	}
}
