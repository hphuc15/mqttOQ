/* PUBLISH - Publish message */
package packet

import (
	"bytes"
)

type PublishPacket struct {
	FixedHeader
	TopicName        string
	PacketIdentifier uint16
	Payload          []byte
}

func (p *PublishPacket) Encode() ([]byte, error) {
	e := p.validate()
	if e != nil {
		return nil, e
	}
	var bodyBytes bytes.Buffer
	// flagsByte := flagMask(p.UserNameFlag)<<7 | flagMask(p.PasswordFlag)<<6 | flagMask(p.WillRetain)<<5 | p.WillQos<<3 | flagMask(p.WillFlag)<<2 | flagMask(p.CleanSession)<<1

	/* Variable header */
	bodyBytes.Write(encodeString(p.TopicName))
	if p.Qos > 0 {
		bodyBytes.Write(encodeUint16(p.PacketIdentifier))
	}

	/* Payload */
	bodyBytes.Write(p.Payload)

	/* Fixed header */
	fixedHeaderBytes, e := p.encodeFixedHeader(bodyBytes.Len())
	if e != nil {
		return nil, e
	}

	var publishPacketBytes bytes.Buffer
	publishPacketBytes.Write(fixedHeaderBytes)
	publishPacketBytes.Write(bodyBytes.Bytes())
	return publishPacketBytes.Bytes(), e
}

func (p *PublishPacket) validate() error {
	// if p.WillQos > 2 {
	// 	return fmt.Errorf("invalid will QoS: %d", p.WillQos)
	// }
	// if p.WillFlag && p.WillTopic == "" {
	// 	return errors.New("will flag set but will topic is empty")
	// }
	// if !p.WillFlag && (p.WillQos != 0 || p.WillRetain) {
	// 	return errors.New("will QoS/retain set without will flag")
	// }
	// if p.PasswordFlag && !p.UserNameFlag {
	// 	return errors.New("password flag requires username flag")
	// }
	return nil
}
