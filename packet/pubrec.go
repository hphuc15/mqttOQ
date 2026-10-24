package packet

import (
	"errors"
	"io"
)

// PubrecPacket represents an MQTT PUBREC packet (QoS 2 step 1).
type PubrecPacket struct {
	PacketIdentifier uint16
}

func (p *PubrecPacket) Type() ControlPacketType {
	return PUBREC
}

func (p *PubrecPacket) Flags() byte {
	return 0
}

func (p *PubrecPacket) Encode(w io.Writer) error {
	e := EncodeFixedHeader(w, PUBREC, 0, 2)
	if e != nil {
		return e
	}
	return writeUint16(w, p.PacketIdentifier)
}

func (p *PubrecPacket) Decode(r io.Reader, remainLength uint32) error {
	if remainLength != 2 {
		return errors.New("malformed PUBREC: remaining length must be 2")
	}
	var err error
	p.PacketIdentifier, err = readUint16(r)
	return err
}
