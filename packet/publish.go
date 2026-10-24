package packet

import (
	"bytes"
	"errors"
	"io"
	"strings"
)

type QualityOfService byte

const (
	QoS0 QualityOfService = 0x00
	QoS1 QualityOfService = 0x01
	QoS2 QualityOfService = 0x02
)

type PublishOthersConfig struct {
	QosLevel         QualityOfService
	FixedHeaderFlags byte // Raw flags from fixed header (used when reading)
	DupFlag          bool
	Retain           bool
}

// Body (Variable Header + Payload) of PUBLISH Control Packet
type PublishPacket struct {
	Config           PublishOthersConfig // This not be included in ControlPacket, just the way to pass some information from Fixed Header.
	TopicName        string
	PacketIdentifier uint16
	Payload          []byte
}

// Type returns the PUBLISH packet type.
func (p *PublishPacket) Type() ControlPacketType {
	return PUBLISH
}

// Flags returns the PUBLISH flags (DUP, QoS, RETAIN).
func (p *PublishPacket) Flags() byte {
	return flagBoolToBitMask(p.Config.DupFlag, 3) | byte(p.Config.QosLevel)<<1 | flagBoolToBitMask(p.Config.Retain, 0)
}

// Encode writes the PUBLISH packet to the writer.
func (p *PublishPacket) Encode(w io.Writer) error {
	if p.Config.QosLevel > QoS2 {
		return errors.New("invalid PUBLISH: QoS")
	}
	if p.Config.QosLevel == QoS0 && p.Config.DupFlag {
		return errors.New("invalid PUBLISH: DUP set with QoS 0")
	}
	if p.TopicName == "" || strings.ContainsAny(p.TopicName, "#+") {
		return errors.New("invalid PUBLISH: topic name")
	}
	if p.Config.QosLevel > QoS0 && p.PacketIdentifier == 0 {
		return errors.New("invalid PUBLISH: zero packet identifier")
	}

	var body bytes.Buffer
	var e error

	e = encodeString(&body, p.TopicName)
	if e != nil {
		return e
	}

	if p.Config.QosLevel == QoS1 || p.Config.QosLevel == QoS2 {
		e = writeUint16(&body, p.PacketIdentifier)
		if e != nil {
			return e
		}
	}

	_, e = body.Write(p.Payload)
	if e != nil {
		return e
	}

	/* Fixed Header */
	pflags := p.Flags()
	remainLength := uint32(body.Len())
	e = EncodeFixedHeader(w, PUBLISH, pflags, remainLength)
	if e != nil {
		return e
	}

	/* Variable Header + Payload */
	_, e = body.WriteTo(w)
	if e != nil {
		return e
	}

	return nil
}

// Decode reads the PUBLISH packet variable header and payload.
func (p *PublishPacket) Decode(r io.Reader, remainLength uint32) error {

	p.Config.DupFlag = flagBitMaskToBool(p.Config.FixedHeaderFlags, 3)
	p.Config.QosLevel = QualityOfService((p.Config.FixedHeaderFlags >> 1) & 0x03)
	p.Config.Retain = flagBitMaskToBool(p.Config.FixedHeaderFlags, 0)
	if p.Config.QosLevel > QoS2 {
		return errors.New("malformed PUBLISH: invalid QoS")
	}

	if p.Config.QosLevel == QoS0 && p.Config.DupFlag {
		return errors.New("malformed PUBLISH: DUP set with QoS 0")
	}

	lr := &io.LimitedReader{R: r, N: int64(remainLength)}

	var e error
	p.TopicName, e = decodeString(lr)
	if e != nil {
		return e
	}

	/* The Topic Name in the PUBLISH Packet MUST NOT contain wildcard characters [MQTT-3.3.2-2]. */
	if p.TopicName == "" || strings.ContainsAny(p.TopicName, "#+") {
		return errors.New("malformed PUBLISH: invalid topic name")
	}

	if p.Config.QosLevel == 1 || p.Config.QosLevel == 2 {
		p.PacketIdentifier, e = readUint16(lr)
		if e != nil {
			return e
		}
		/* Control Packets MUST contain a non-zero 16-bit Packet Identifier [MQTT-2.3.1-1] */
		if p.PacketIdentifier == 0 {
			return errors.New("malformed PUBLISH: zero packet identifier")
		}
	}

	p.Payload = make([]byte, lr.N)
	_, e = io.ReadFull(lr, p.Payload)
	return e
}
