package transport

import "crypto/tls"

// DefaultALPN is the standard ALPN identifier for MQTT over QUIC.
const DefaultALPN = "mqtt"

func NewClientTLSConfig(insecureSkipVerify bool, serverName string, alpn ...string) *tls.Config {
	alpns := alpn
	if len(alpns) == 0 {
		alpns = []string{DefaultALPN}
	}

	return &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkipVerify,
		NextProtos:         alpns,
		MinVersion:         tls.VersionTLS13,
	}
}
