/* PUBCOMP - Publish complete (QoS 2 publish received, part 3) */
package packet

type PubcompPacket struct {
	FixedHeader
	PacketIdentifier uint16
}

func (p *PubcompPacket) CreateDefault() error {

	return nil
}
