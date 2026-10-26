package transport

import (
	"context"

	"github.com/quic-go/quic-go"
)

// OpenStreamSync opens a bidirectional QUIC stream on the given connection and wraps it in StreamConn.
func OpenStreamSync(ctx context.Context, conn *quic.Conn) (*StreamConn, error) {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return NewStreamConn(stream, conn), nil
}
