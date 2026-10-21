package mqttOQ

import (
	"github.com/quic-go/quic-go"
)

const (
	ConnectionCloseNormal        quic.ApplicationErrorCode = 0x00
	ConnectionCloseConnectFailed quic.ApplicationErrorCode = 0x01
)
