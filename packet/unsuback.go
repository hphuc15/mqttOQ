package packet

import (
	"errors"
	"io"
)

type UnsubackPacket struct {
	PacketIdentifier uint16
}

func (p *UnsubackPacket) Type() ControlPacketType {
	return UNSUBACK
}

func (p *UnsubackPacket) Flags() byte {
	return 0x00
}

func (p *UnsubackPacket) Encode(w io.Writer) error {
	e := EncodeFixedHeader(w, UNSUBACK, 0x00, 0x02)
	if e != nil {
		return e
	}

	return writeUint16(w, p.PacketIdentifier)
}

func (p *UnsubackPacket) Decode(r io.Reader, remainLength uint32) error {
	if remainLength != 2 {
		return errors.New("malformed UNSUBACK packet: remaining length must be 2")
	}

	var e error
	p.PacketIdentifier, e = readUint16(r)
	return e
}
