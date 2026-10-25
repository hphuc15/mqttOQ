package packet

import "io"

// PingrespPacket represents an MQTT PINGRESP packet.
type PingrespPacket struct{}

// Type returns the PINGRESP packet type.
func (p *PingrespPacket) Type() ControlPacketType {
	return PINGRESP
}

// Flags returns the PINGRESP flags (always 0).
func (p *PingrespPacket) Flags() byte {
	return 0
}

// Encode writes the PINGRESP packet to the writer.
func (p *PingrespPacket) Encode(w io.Writer) error {
	return EncodeFixedHeader(w, PINGRESP, 0, 0)
}

// Decode reads the PINGRESP packet (no payload).
func (p *PingrespPacket) Decode(r io.Reader, remainLength uint32) error {
	return nil
}
