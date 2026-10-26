package client

import (
	"fmt"
	"sync/atomic"
)

// ConnectionState represents the lifecycle state of the MQTT over QUIC client.
type ConnectionState int32

const (
	StateDisconnected ConnectionState = 0
	StateConnecting   ConnectionState = 1
	StateConnected    ConnectionState = 2
	StateClosed       ConnectionState = 3
)

// String returns the string representation of ConnectionState.
func (s ConnectionState) String() string {
	switch s {
	case StateDisconnected:
		return "Disconnected"
	case StateConnecting:
		return "Connecting"
	case StateConnected:
		return "Connected"
	case StateClosed:
		return "Closed"
	default:
		return fmt.Sprintf("Unknown(%d)", int32(s))
	}
}

// atomicState manages atomic operations on ConnectionState.
type atomicState struct {
	state int32
}

func newAtomicState(initial ConnectionState) *atomicState {
	return &atomicState{
		state: int32(initial),
	}
}

// Load atomically retrieves the current connection state.
func (a *atomicState) Load() ConnectionState {
	return ConnectionState(atomic.LoadInt32(&a.state))
}

// CompareAndSwap executes an atomic compare-and-swap on connection state.
func (a *atomicState) CompareAndSwap(oldState, newState ConnectionState) bool {
	return atomic.CompareAndSwapInt32(&a.state, int32(oldState), int32(newState))
}

// Set atomically stores the new connection state.
func (a *atomicState) Set(newState ConnectionState) {
	atomic.StoreInt32(&a.state, int32(newState))
}
