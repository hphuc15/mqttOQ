package packet

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

func writeUint8(w io.Writer, n uint8) error {
	_, e := w.Write([]byte{n})
	if e != nil {
		return e
	}
	return nil
}

func writeUint16(w io.Writer, n uint16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], n)
	_, e := w.Write(buf[:])
	return e
}

func readUint16(r io.Reader) (uint16, error) {
	var buf [2]byte
	_, e := io.ReadFull(r, buf[:])
	if e != nil {
		return 0, e
	}

	return binary.BigEndian.Uint16(buf[:]), nil
}

func writeString(w io.Writer, s string) error {
	length := uint16(len(s))

	// Write string content
	if length > 0 {
		_, e := w.Write([]byte(s))
		return e
	}
	return nil
}

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	_, e := io.ReadFull(r, b[:])
	if e != nil {
		return 0, e
	}
	return b[0], nil
}

func encodeString(w io.Writer, s string) error {
	length := len(s)
	if length > math.MaxUint16 {
		return fmt.Errorf("string too long: %d bytes (max %d)", len(s), math.MaxUint16)
	}
	// Write string length
	e := writeUint16(w, uint16(length))
	if e != nil {
		return e
	}
	// Write string content
	if length > 0 {
		_, e = w.Write([]byte(s))
		return e
	}
	return nil
}

func flagBitMaskToBool(b byte, position uint8) bool {
	return ((b >> position) & 0x01) == 1
}

func flagBoolToBitMask(flag bool, position uint8) byte {
	if flag {
		return 1 << position
	}
	return 0
}

// encodeBytes writes MQTT Binary Data: a Two Byte Integer length
// followed by that many bytes (0 to 65,535).
func encodeBytes(w io.Writer, data []byte) error {
	length := len(data)
	if length > math.MaxUint16 {
		return fmt.Errorf("binary data too long: %d bytes (max %d)", length, math.MaxUint16)
	}

	e := writeUint16(w, uint16(length))
	if e != nil {
		return e
	}

	if length > 0 {
		_, e = w.Write(data)
		return e
	}
	return nil
}

// decodeBytes reads MQTT Binary Data: a Two Byte Integer length
// followed by that many bytes.
func decodeBytes(r io.Reader) ([]byte, error) {
	length, e := readUint16(r)
	if e != nil {
		return nil, e
	}
	buf := make([]byte, length)
	if length > 0 {
		if _, e = io.ReadFull(r, buf); e != nil {
			return nil, e
		}
	}
	return buf, nil
}

func decodeString(r io.Reader) (string, error) {
	length, e := readUint16(r)
	if e != nil {
		return "", e
	}
	if length == 0 {
		return "", nil
	}
	buf := make([]byte, length)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return "", e
	}
	return string(buf), nil
}
