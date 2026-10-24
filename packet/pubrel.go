package packet

import (
	"errors"
	"io"
)

// PubrelPacket represents an MQTT PUBREL packet (QoS 2 step 2).
type PubrelPacket struct {
	PacketIdentifier uint16
}

func (p *PubrelPacket) Type() ControlPacketType {
	return PUBREL
}

func (p *PubrelPacket) Flags() byte {
	return 0x02
} // Bit 1 is reserved and must be set to 1

func (p *PubrelPacket) Encode(w io.Writer) error {
	e := EncodeFixedHeader(w, PUBREL, 0x02, 2)
	if e != nil {
		return e
	}
	return writeUint16(w, p.PacketIdentifier)
}

func (p *PubrelPacket) Decode(r io.Reader, remainLength uint32) error {
	if remainLength != 2 {
		return errors.New("malformed PUBREL: remaining length must be 2")
	}
	var e error
	p.PacketIdentifier, e = readUint16(r)
	return e
}
