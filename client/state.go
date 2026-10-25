package client

import "sync/atomic"

// ConnectionState represents the lifecycle state of the MQTT over QUIC client.
type ConnectionState int32

const (
	StateDisconnected ConnectionState = 0
	StateConnecting   ConnectionState = 1
	StateConnected    ConnectionState = 2
	StateClosed       ConnectionState = 3
)

// atomicState manages atomic operations on ConnectionState.
type atomicState struct {
	state int32
}

func (a *atomicState) CompareAndSwap(oldState, newState ConnectionState) bool {
	return atomic.CompareAndSwapInt32(&a.state, int32(oldState), int32(newState))
}

func (a *atomicState) Set(newState ConnectionState) {
	atomic.StoreInt32(&a.state, int32(newState))
}
