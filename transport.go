// MQTT: 3.1.1
package mqttOQ

import (
	"crypto/tls"

	"github.com/quic-go/quic-go"
)

type Transport struct {
	quicConfig *quic.Config
	tlsConfig  *tls.Config
}
