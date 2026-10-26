package client

import (
	"context"
	"time"

	"github.com/hphuc15/mqttOQ/packet"
)

// startKeepAlive launches a background goroutine that sends PINGREQ at the
// configured keep-alive interval and waits for a PINGRESP. If no response
// arrives within the same interval, the connection is terminated.
//
// The goroutine exits when closeCh is closed.
func (c *Client) startKeepAlive() {
	interval := time.Duration(c.config.Session.KeepAlive) * time.Second
	if interval == 0 {
		// Keep-alive is disabled; nothing to do.
		return
	}

	c.pingRespCh = make(chan struct{}, 1)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.keepAliveLoop(interval)
	}()
}

// keepAliveLoop is the internal ticker loop. It is extracted for testability.
func (c *Client) keepAliveLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			return

		case <-ticker.C:
			// Send PINGREQ
			if err := c.sendControlPacket(&packet.PingreqPacket{}); err != nil {
				// Transport error: trigger disconnect and exit.
				c.handleUnexpectedDisconnect()
				return
			}

			// Wait for PINGRESP within the same interval.
			select {
			case <-c.pingRespCh:
				// Received PINGRESP; connection is alive.

			case <-time.After(interval):
				// Timeout: broker did not respond.
				c.handleUnexpectedDisconnect()
				return

			case <-c.closeCh:
				return
			}
		}
	}
}

// Disconnect sends an MQTT DISCONNECT packet to the broker and performs a
// clean shutdown of all background goroutines and transport resources.
// After Disconnect returns, the client is in StateDisconnected and safe to
// either discard or reconnect.
func (c *Client) Disconnect(ctx context.Context) error {
	if !c.state.CompareAndSwap(StateConnected, StateClosed) {
		// If state is not Connected, we cannot perform a clean disconnect.
		return ErrNotConnected
	}

	c.stopOnce.Do(func() {
		close(c.stopCh)
	})

	// Best-effort send of the DISCONNECT packet; ignore write errors because
	// we will close the transport immediately afterwards.
	_ = c.sendControlPacket(&packet.DisconnectPacket{})

	// Signal all background goroutines (readLoop, keepAliveLoop) to exit.
	c.cleanupTransport()

	// Wait for all goroutines to finish before returning.
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines exited cleanly.
	case <-ctx.Done():
		// Caller's context expired; goroutines will still exit eventually.
		return ctx.Err()
	}

	c.state.Set(StateDisconnected)
	return nil
}

// Close permanently shuts down the client, canceling any pending reconnection
// and freeing transport resources.
func (c *Client) Close() error {
	c.state.Set(StateClosed)
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	c.cleanupTransport()
	c.state.Set(StateDisconnected)
	return nil
}
