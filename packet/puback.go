/* PUBACK - Publish acknowledgement */
package packet

type PubackPacket struct {
	FixedHeader
	PacketIdentifier uint16
}

func (p *PubackPacket) CreateDefault() error {

	return nil
}
