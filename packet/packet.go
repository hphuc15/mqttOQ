package packet

import (
	"errors"
	"io"
)

type ControlPacketType byte

const (
	CONNECT     ControlPacketType = 0x01 // Client request to connect to Server
	CONNACK     ControlPacketType = 0x02 // Connect acknowledgment
	PUBLISH     ControlPacketType = 0x03 // Publish message
	PUBACK      ControlPacketType = 0x04 // Publish acknowledgment
	PUBREC      ControlPacketType = 0x05 // Publish received (assured delivery part 1)
	PUBREL      ControlPacketType = 0x06 // Publish release (assured delivery part 2)
	PUBCOMP     ControlPacketType = 0x07 // Publish complete (assured delivery part 3)
	SUBSCRIBE   ControlPacketType = 0x08 // Client subscribe request
	SUBACK      ControlPacketType = 0x09 // Subscribe acknowledgment
	UNSUBSCRIBE ControlPacketType = 0x0A // Unsubscribe request
	UNSUBACK    ControlPacketType = 0x0B // Unsubscribe acknowledgment
	PINGREQ     ControlPacketType = 0x0C // PING request
	PINGRESP    ControlPacketType = 0x0D // PING response
	DISCONNECT  ControlPacketType = 0x0E // Client is disconnecting
)

type FixedHeader struct {
	Type         byte
	Flags        byte
	RemainLength uint32
}

type ControlPacket interface {
	// Encode writes the entire packet (Fixed Header + Variable Header + Payload) into the writer.
	Encode(w io.Writer) error
	// Decode reads the packet's fixed header, variable header and payload from reader with given remaining length.
	Decode(r io.Reader, remainLength uint32) error
}

// DecodeRemainLength reads an MQTT Variable Byte Integer from the reader.
func DecodeRemainLength(r io.Reader) (uint32, error) {
	var multiplier uint32 = 1
	var value uint32 = 0
	var singleByte [1]byte

	for i := 0; i < 4; i++ {
		if _, e := io.ReadFull(r, singleByte[:]); e != nil {
			return 0, e
		}
		encodedByte := singleByte[0]
		value += uint32(encodedByte&127) * multiplier

		if (encodedByte & 128) == 0 {
			return value, nil
		}

		multiplier *= 128
	}

	return 0, ErrVarintOverflow
}

const maxVarintValue = 268435455 // (128^4) - 1

// ErrVarintOverflow is returned when remaining length exceeds 4 bytes or 268,435,455.
var ErrVarintOverflow = errors.New("MQTT variable length integer exceeds maximum allowed 4 bytes")

func EncodeRemainLength(w io.Writer, val uint32) error {
	if val > maxVarintValue {
		return ErrVarintOverflow
	}

	var buf [4]byte
	idx := 0
	for {
		encodedByte := byte(val % 128)
		val /= 128
		if val > 0 {
			encodedByte |= 128
		}
		buf[idx] = encodedByte
		idx++
		if val == 0 {
			break
		}
	}

	_, e := w.Write(buf[:idx])
	return e
}

func DecodeFixedHeader(r io.Reader) (*FixedHeader, error) {
	var typeAndFlagsByte [1]byte
	_, e := io.ReadFull(r, typeAndFlagsByte[:])
	if e != nil {
		return nil, e
	}

	remainLength, e := DecodeRemainLength(r)
	if e != nil {
		return nil, e
	}

	controlPacketType := (typeAndFlagsByte[0] >> 4) & 0x0F
	flags := typeAndFlagsByte[0] & 0x0F

	return &FixedHeader{
		Type:         controlPacketType,
		Flags:        flags,
		RemainLength: remainLength,
	}, nil
}

// EncodeFixedHeader writes the Fixed Header (Type, Flags, Remaining Length) to the writer.
func EncodeFixedHeader(w io.Writer, pType ControlPacketType, flags byte, remainLength uint32) error {
	typeAndFlagsByte := ((byte(pType) << 4) & 0xF0) | (flags & 0x0F)
	_, e := w.Write([]byte{typeAndFlagsByte})
	if e != nil {
		return e
	}
	return EncodeRemainLength(w, remainLength)
}
