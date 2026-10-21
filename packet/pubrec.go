/* PUBREC - Publish received (QoS 2 publish received, part 1) */
package packet

type PubrecPacket struct {
	FixedHeader
	PacketIdentifier uint16
}

func (p *PubrecPacket) CreateDefault() error {

	return nil
}
