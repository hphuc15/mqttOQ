package transport

import (
	"context"
	"crypto/tls"

	"github.com/quic-go/quic-go"
)

// Dial initiates a standard 1-RTT QUIC connection to the target address.
func Dial(ctx context.Context, addr string, tlsConfig *tls.Config, quicConfig *quic.Config) (*quic.Conn, error) {
	if tlsConfig == nil {
		tlsConfig = NewClientTLSConfig(true, "localhost")
	}
	if quicConfig == nil {
		quicConfig = &quic.Config{
			EnableDatagrams: false,
		}
	}

	conn, err := quic.DialAddr(ctx, addr, tlsConfig, quicConfig)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// DialEarly initiates a 0-RTT QUIC connection to the target address if session tickets are available.
func DialEarly(ctx context.Context, addr string, tlsConfig *tls.Config, quicConfig *quic.Config) (*quic.Conn, error) {
	if tlsConfig == nil {
		tlsConfig = NewClientTLSConfig(true, "localhost")
	}
	if quicConfig == nil {
		quicConfig = &quic.Config{
			EnableDatagrams: false,
		}
	}

	conn, err := quic.DialAddrEarly(ctx, addr, tlsConfig, quicConfig)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Listen starts a QUIC listener on the given address.
func Listen(addr string, tlsConfig *tls.Config, quicConfig *quic.Config) (*quic.Listener, error) {
	if quicConfig == nil {
		quicConfig = &quic.Config{
			EnableDatagrams: false,
		}
	}
	return quic.ListenAddr(addr, tlsConfig, quicConfig)
}
