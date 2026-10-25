package packet

import "io"

// PingreqPacket represents an MQTT PINGREQ packet.
type PingreqPacket struct{}

// Type returns the PINGREQ packet type.
func (p *PingreqPacket) Type() ControlPacketType {
	return PINGREQ
}

// Flags returns the PINGREQ flags (always 0).
func (p *PingreqPacket) Flags() byte {
	return 0x00
}

// Encode writes the PINGREQ packet to the writer.
func (p *PingreqPacket) Encode(w io.Writer) error {
	return EncodeFixedHeader(w, PINGREQ, 0x00, 0)
}

// Decode reads the PINGREQ packet (no payload).
func (p *PingreqPacket) Decode(r io.Reader, remainLength uint32) error {
	return nil
}
