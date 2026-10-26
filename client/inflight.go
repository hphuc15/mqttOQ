package client

import (
	"sync"

	"github.com/hphuc15/mqttOQ/packet"
)

// inflightState represents the acknowledgement phase of a QoS 1 or QoS 2 packet.
type inflightState int

const (
	// inflightWaitPuback indicates QoS 1 PUBLISH sent, awaiting PUBACK.
	inflightWaitPuback inflightState = iota

	// inflightWaitPubrec indicates QoS 2 PUBLISH sent, awaiting PUBREC.
	inflightWaitPubrec

	// inflightWaitPubcomp indicates QoS 2 PUBREL sent, awaiting PUBCOMP.
	inflightWaitPubcomp
)

// inflightMessage tracks an in-flight unacknowledged publish message.
type inflightMessage struct {
	packetID uint16
	publish  *packet.PublishPacket
	state    inflightState
}

// inflightManager manages QoS 1 and QoS 2 messages awaiting acknowledgement.
type inflightManager struct {
	mu       sync.RWMutex
	messages map[uint16]*inflightMessage
	inbound  map[uint16]struct{}
}

func newInflightManager() *inflightManager {
	return &inflightManager{
		messages: make(map[uint16]*inflightMessage),
		inbound:  make(map[uint16]struct{}),
	}
}

// add inserts an in-flight message into the manager.
func (m *inflightManager) add(pid uint16, pkt *packet.PublishPacket, state inflightState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messages[pid] = &inflightMessage{
		packetID: pid,
		publish:  pkt,
		state:    state,
	}
}

// remove deletes an in-flight message once fully acknowledged.
func (m *inflightManager) remove(pid uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.messages, pid)
}

// updateState updates the acknowledgement phase of an in-flight message.
func (m *inflightManager) updateState(pid uint16, state inflightState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if msg, ok := m.messages[pid]; ok {
		msg.state = state
	}
}

// count returns the number of active in-flight messages.
func (m *inflightManager) count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.messages)
}

// get returns a copy of an inflightMessage pointer or nil if not found.
func (m *inflightManager) get(pid uint16) (*inflightMessage, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	msg, ok := m.messages[pid]
	return msg, ok
}

// contains reports whether the given packet ID is currently tracked as in-flight.
func (m *inflightManager) contains(pid uint16) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.messages[pid]
	return ok
}

// retryAll resends all in-flight messages over the provided sendFn.
// Per MQTT 3.1.1 Section 3.3.1.1:
// - Retried PUBLISH packets MUST have the DUP flag set to 1.
// - QoS 2 messages in step 2 (waiting PUBCOMP) resend PUBREL.
func (m *inflightManager) retryAll(sendFn func(packet.ControlPacket) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, msg := range m.messages {
		switch msg.state {
		case inflightWaitPuback, inflightWaitPubrec:
			if msg.publish != nil {
				dupPkt := *msg.publish
				dupPkt.Config.DupFlag = true
				if err := sendFn(&dupPkt); err != nil {
					return err
				}
			}
		case inflightWaitPubcomp:
			pubrel := &packet.PubrelPacket{PacketIdentifier: msg.packetID}
			if err := sendFn(pubrel); err != nil {
				return err
			}
		}
	}
	return nil
}

// addInbound records an inbound QoS 2 packet ID awaiting PUBREL.
// It returns true if the packet ID was not previously recorded (new message),
// or false if it was already recorded (duplicate message).
func (m *inflightManager) addInbound(pid uint16) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.inbound[pid]; exists {
		return false
	}
	m.inbound[pid] = struct{}{}
	return true
}

// hasInbound reports whether the given packet ID is currently tracked as an uncompleted inbound QoS 2 message.
func (m *inflightManager) hasInbound(pid uint16) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.inbound[pid]
	return exists
}

// removeInbound deletes the tracked inbound QoS 2 packet ID upon receiving PUBREL.
func (m *inflightManager) removeInbound(pid uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.inbound, pid)
}

// inboundCount returns the number of active inbound QoS 2 packets awaiting PUBREL.
func (m *inflightManager) inboundCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.inbound)
}

// clearInbound removes all tracked inbound QoS 2 packet IDs.
func (m *inflightManager) clearInbound() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.inbound = make(map[uint16]struct{})
}
