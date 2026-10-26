package transport

import (
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// StreamConn wraps a QUIC stream and its parent QUIC connection to implement net.Conn.
type StreamConn struct {
	stream    *quic.Stream
	conn      *quic.Conn
	closeOnce sync.Once
	closeErr  error
}

// NewStreamConn creates a new StreamConn instance.
func NewStreamConn(stream *quic.Stream, conn *quic.Conn) *StreamConn {
	return &StreamConn{
		stream: stream,
		conn:   conn,
	}
}

// Stream returns the underlying QUIC stream.
func (s *StreamConn) Stream() *quic.Stream {
	return s.stream
}

// Conn returns the underlying QUIC connection.
func (s *StreamConn) Conn() *quic.Conn {
	return s.conn
}

// Read reads data from the stream.
func (s *StreamConn) Read(b []byte) (int, error) {
	if s.stream == nil {
		return 0, net.ErrClosed
	}
	return s.stream.Read(b)
}

// Write writes data to the stream.
func (s *StreamConn) Write(b []byte) (int, error) {
	if s.stream == nil {
		return 0, net.ErrClosed
	}
	return s.stream.Write(b)
}

// CloseStream closes only the stream's write side without tearing down the underlying connection.
func (s *StreamConn) CloseStream() error {
	if s.stream != nil {
		return s.stream.Close()
	}
	return nil
}

// Close closes the stream and terminates the underlying QUIC connection.
func (s *StreamConn) Close() error {
	s.closeOnce.Do(func() {
		if s.stream != nil {
			_ = s.stream.Close()
		}
		if s.conn != nil {
			s.closeErr = s.conn.CloseWithError(0, "connection closed")
		}
	})
	return s.closeErr
}

// LocalAddr returns the local network address.
func (s *StreamConn) LocalAddr() net.Addr {
	if s.conn != nil {
		return s.conn.LocalAddr()
	}
	return nil
}

// RemoteAddr returns the remote network address.
func (s *StreamConn) RemoteAddr() net.Addr {
	if s.conn != nil {
		return s.conn.RemoteAddr()
	}
	return nil
}

// SetDeadline sets both read and write deadlines on the stream.
func (s *StreamConn) SetDeadline(t time.Time) error {
	if s.stream != nil {
		return s.stream.SetDeadline(t)
	}
	return nil
}

// SetReadDeadline sets the read deadline on the stream.
func (s *StreamConn) SetReadDeadline(t time.Time) error {
	if s.stream != nil {
		return s.stream.SetReadDeadline(t)
	}
	return nil
}

// SetWriteDeadline sets the write deadline on the stream.
func (s *StreamConn) SetWriteDeadline(t time.Time) error {
	if s.stream != nil {
		return s.stream.SetWriteDeadline(t)
	}
	return nil
}
