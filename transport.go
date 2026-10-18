package mqttOQ

import (
	"github.com/quic-go/quic-go"
)

type Transport struct {
	quicConfig *quic.Config
}
