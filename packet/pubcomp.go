package packet

import (
	"errors"
	"io"
)

// PubcompPacket represents an MQTT PUBCOMP packet (QoS 2 step 3).
type PubcompPacket struct {
	PacketIdentifier uint16
}

func (p *PubcompPacket) Type() ControlPacketType {
	return PUBCOMP
}

func (p *PubcompPacket) Flags() byte {
	return 0
}

func (p *PubcompPacket) Encode(w io.Writer) error {
	e := EncodeFixedHeader(w, PUBCOMP, 0, 2)
	if e != nil {
		return e
	}
	return writeUint16(w, p.PacketIdentifier)
}

func (p *PubcompPacket) Decode(r io.Reader, remainLength uint32) error {
	if remainLength != 2 {
		return errors.New("malformed PUBCOMP: remaining length must be 2")
	}
	var e error
	p.PacketIdentifier, e = readUint16(r)
	return e
}
