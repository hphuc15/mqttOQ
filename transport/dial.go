package transport

import (
	"context"
	"crypto/tls"

	"github.com/quic-go/quic-go"
)

// type ClientConnection struct {
// 	conn *quic.Conn
// }

// Dial initiates a QUIC connection to the target address
func Dial(ctx context.Context, addr string, tlsConfig *tls.Config, quicConfig *quic.Config) (*quic.Conn, error) {
	if tlsConfig == nil {
		tlsConfig = NewClientTLSConfig(true, "mqtt")
	}
	if quicConfig == nil {
		quicConfig = &quic.Config{
			EnableDatagrams: false,
		}
	}

	conn, e := quic.DialAddr(ctx, addr, tlsConfig, quicConfig)
	if e != nil {
		return nil, e
	}
	return conn, nil
}

// // OpenStream opens a new bidirectional QUIC stream asynchronously.
// func (c *ClientConnection) OpenStream() (*StreamConn, error) {
// 	stream, err := c.conn.OpenStream()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return NewStreamConn(stream, c.conn), nil
// }

// // OpenStreamSync opens a new bidirectional QUIC stream, waiting if flow control limit is hit.
// func (c *ClientConnection) OpenStreamSync(ctx context.Context) (*StreamConn, error) {
// 	stream, err := c.conn.OpenStreamSync(ctx)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return NewStreamConn(stream, c.conn), nil
// }

// // Close closes the QUIC connection gracefully.
// func (c *ClientConnection) Close() error {
// 	return c.conn.CloseWithError(0, "client closed")
// }
