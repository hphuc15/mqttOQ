package packet

import "io"

// DisconnectPacket represents an MQTT DISCONNECT packet.
type DisconnectPacket struct{}

// Type returns the DISCONNECT packet type.
func (p *DisconnectPacket) Type() ControlPacketType {
	return DISCONNECT
}

// Flags returns the DISCONNECT flags (always 0).
func (p *DisconnectPacket) Flags() byte {
	return 0x00
}

// Encode writes the DISCONNECT packet to the writer.
func (p *DisconnectPacket) Encode(w io.Writer) error {
	return EncodeFixedHeader(w, DISCONNECT, 0x00, 0)
}

// Decode reads the DISCONNECT packet (no payload).
func (p *DisconnectPacket) Decode(r io.Reader, remainLength uint32) error {
	return nil
}
