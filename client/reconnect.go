package client

import (
	"context"
	"time"

	"github.com/hphuc15/mqttOQ/packet"
)

// calculateBackoff calculates the exponential backoff duration for a given reconnect attempt.
// Formula: interval = min(ReconnectInterval * 2^attempt, MaxReconnectInterval)
func (c *Client) calculateBackoff(attempt int) time.Duration {
	initial := c.config.Network.ReconnectInterval
	if initial <= 0 {
		initial = 1 * time.Second
	}

	maxInterval := c.config.Network.MaxReconnectInterval
	if maxInterval <= 0 {
		maxInterval = 30 * time.Second
	}

	backoff := initial
	for i := 0; i < attempt; i++ {
		backoff *= 2
		if backoff >= maxInterval {
			return maxInterval
		}
	}

	if backoff > maxInterval {
		return maxInterval
	}
	return backoff
}

// triggerReconnect launches the background reconnect loop if not already running.
func (c *Client) triggerReconnect() {
	c.reconnectMu.Lock()
	if c.isReconnecting || c.state.Load() == StateClosed {
		c.reconnectMu.Unlock()
		return
	}
	c.isReconnecting = true
	c.reconnectMu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() {
			c.reconnectMu.Lock()
			c.isReconnecting = false
			c.reconnectMu.Unlock()
		}()
		c.reconnectLoop()
	}()
}

// reconnectLoop manages the exponential backoff reconnection attempts until connected or permanently closed.
func (c *Client) reconnectLoop() {
	attempt := 0

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if c.state.Load() == StateClosed {
			return
		}

		backoff := c.calculateBackoff(attempt)

		select {
		case <-c.stopCh:
			return
		case <-time.After(backoff):
		}

		if c.state.Load() == StateClosed {
			return
		}

		c.state.Set(StateConnecting)

		timeout := c.config.Network.ConnectTimeout
		if timeout <= 0 {
			timeout = ClientDefaultConnectTimeout
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		var err error
		if c.config.Network.Enable0RTT {
			err = c.connectTransportEarly(ctx)
		} else {
			err = c.connectTransport(ctx)
		}
		cancel()

		if err == nil {
			// Successfully reconnected
			c.startReaderLoop()
			c.startKeepAlive()

			// Resubscribe active subscriptions
			c.resubscribeActive()

			// Retry pending QoS 1 and QoS 2 in-flight messages
			c.resendInflightMessages()

			c.state.Set(StateConnected)
			return
		}

		c.state.Set(StateDisconnected)
		attempt++
	}
}

// resubscribeActive restores all registered subscriptions after reconnecting.
func (c *Client) resubscribeActive() {
	c.subMu.RLock()
	if len(c.subscriptions) == 0 {
		c.subMu.RUnlock()
		return
	}

	subs := make([]packet.TopicSubscription, 0, len(c.subscriptions))
	for topic, qos := range c.subscriptions {
		subs = append(subs, packet.TopicSubscription{
			TopicFilter:  topic,
			RequestedQos: packet.QualityOfService(qos),
		})
	}
	c.subMu.RUnlock()

	pid := c.nextPacketID()
	ackCh := c.registerAckWaiter(pid)
	defer c.unregisterAckWaiter(pid)

	pkt := &packet.SubscribePacket{
		PacketIdentifier: pid,
		Subscriptions:    subs,
	}

	if err := c.sendPacket(pkt); err != nil {
		return
	}

	timeout := c.config.Network.ConnectTimeout
	if timeout <= 0 {
		timeout = ClientDefaultConnectTimeout
	}

	select {
	case <-ackCh:
	case <-time.After(timeout):
	case <-c.closeCh:
	case <-c.stopCh:
	}
}

// resendInflightMessages retries all unacknowledged QoS 1 and QoS 2 messages.
func (c *Client) resendInflightMessages() {
	if c.inflight == nil {
		return
	}
	_ = c.inflight.retryAll(c.sendPacket)
}

// Subscriptions returns a copy of currently tracked active topic subscriptions.
func (c *Client) Subscriptions() map[string]byte {
	c.subMu.RLock()
	defer c.subMu.RUnlock()

	res := make(map[string]byte, len(c.subscriptions))
	for k, v := range c.subscriptions {
		res[k] = v
	}
	return res
}
