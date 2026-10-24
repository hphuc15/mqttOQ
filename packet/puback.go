package packet

import (
	"errors"
	"io"
)

// PubackPacket represents an MQTT PUBACK packet (QoS 1 response).
type PubackPacket struct {
	PacketIdentifier uint16
}

func (p *PubackPacket) Type() ControlPacketType {
	return PUBACK
}

func (p *PubackPacket) Flags() byte {
	return 0
}

func (p *PubackPacket) Encode(w io.Writer) error {
	e := EncodeFixedHeader(w, PUBACK, 0, 2)
	if e != nil {
		return e
	}
	return writeUint16(w, p.PacketIdentifier)
}

func (p *PubackPacket) Decode(r io.Reader, remainLength uint32) error {
	if remainLength != 2 {
		return errors.New("malformed PUBACK: remaining length must be 2")
	}
	var e error
	p.PacketIdentifier, e = readUint16(r)
	return e
}
