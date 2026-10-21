/* SUBSCRIBE - Subscribe to topics */
package packet

type SubscribePacket struct {
	FixedHeader
	PacketIdentifier uint16
	TopicFilter      string
	RequestedQoS     []byte
}

func (p *SubscribePacket) CreateDefault() error {

	return nil
}
