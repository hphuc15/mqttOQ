package transport

import (
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

type StreamConn struct {
	stream *quic.Stream
	conn   *quic.Conn
}

func NewStreamConn(stream *quic.Stream, conn *quic.Conn) *StreamConn {
	return &StreamConn{
		stream: stream,
		conn:   conn,
	}
}

func (s *StreamConn) Read(b []byte) (n int, e error) {
	return s.stream.Read(b)
}

func (s *StreamConn) Write(b []byte) (n int, e error) {
	return s.stream.Write(b)
}

// Close the connection.
func (s *StreamConn) Close() error {
	e := s.stream.Close()
	if e != nil {
		return e
	}
	return s.conn.CloseWithError(0, "")
}

// LocalAddr returns the local network address, if known.
func (s *StreamConn) LocalAddr() net.Addr {
	if s.conn != nil {
		return s.conn.LocalAddr()
	}
	return nil
}

// RemoteAddr returns the remote network address, if known.
func (s *StreamConn) RemoteAddr() net.Addr {
	if s.conn != nil {
		return s.conn.RemoteAddr()
	}
	return nil
}

// SetDeadline sets both read and write deadlines for the stream.
func (s *StreamConn) SetDeadline(t time.Time) error {
	return s.stream.SetDeadline(t)
}

// SetReadDeadline sets the read deadline for the stream.
func (s *StreamConn) SetReadDeadline(t time.Time) error {
	return s.stream.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline for the stream.
func (s *StreamConn) SetWriteDeadline(t time.Time) error {
	return s.stream.SetWriteDeadline(t)
}
