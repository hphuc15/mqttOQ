/* PUBREL - Publish release (QoS 2 publish received, part 2) */
package packet

type PubrelPacket struct {
	FixedHeader
	PacketIdentifier uint16
}

func (p *PubrelPacket) CreateDefault() error {

	return nil
}
