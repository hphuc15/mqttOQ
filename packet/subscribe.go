package packet

import (
	"bytes"
	"errors"
	"io"
)

type TopicSubscription struct {
	TopicFilter  string
	RequestedQos QualityOfService
}

// SubscribePacket represents an MQTT PUBREL packet (QoS 2 step 2).
type SubscribePacket struct {
	PacketIdentifier uint16
	Subscriptions    []TopicSubscription
}

func (p *SubscribePacket) Type() ControlPacketType {
	return SUBSCRIBE
}

func (p *SubscribePacket) Flags() byte {
	/**
	 * Bits 3,2,1 and 0 of the fixed header of the SUBSCRIBE Control Packet are
	 * reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat
	 * any other value as malformed and close the Network Connection [MQTT-3.8.1-1]
	 */
	return 0x02
}

func (p *SubscribePacket) Encode(w io.Writer) error {
	if len(p.Subscriptions) == 0 {
		return errors.New("SUBSCRIBE packet must contain at least one subscription")
	}

	var body bytes.Buffer
	var e error
	e = writeUint16(&body, p.PacketIdentifier)
	if e != nil {
		return e
	}

	for _, sub := range p.Subscriptions {
		e = writeString(&body, sub.TopicFilter)
		if e != nil {
			return e
		}
		e = body.WriteByte(byte(sub.RequestedQos))
		if e != nil {
			return e
		}
	}

	e = EncodeFixedHeader(w, SUBSCRIBE, 0x02, uint32(body.Len()))
	if e != nil {
		return e
	}
	_, e = body.WriteTo(w)
	return e
}

func (p *SubscribePacket) Decode(r io.Reader, remainLength uint32) error {
	if remainLength < 5 { // 2 bytes PacketIdentifier + 2 bytes topic len + at least 1 char topic + 1 byte QoS
		return errors.New("malformed SUBSCRIBE packet: remaining length too short")
	}

	lr := io.LimitReader(r, int64(remainLength))

	var e error
	p.PacketIdentifier, e = readUint16(lr)
	if e != nil {
		return e
	}

	bytesLeft := int(remainLength) - 2
	p.Subscriptions = nil

	for bytesLeft > 0 {
		topic, e := decodeString(lr)
		if e != nil {
			return e
		}
		bytesLeft -= (2 + len(topic))

		var qosBuf [1]byte
		_, e = io.ReadFull(lr, qosBuf[:])
		if e != nil {
			return e
		}
		bytesLeft--

		p.Subscriptions = append(p.Subscriptions, TopicSubscription{
			TopicFilter:  topic,
			RequestedQos: QualityOfService(qosBuf[0]),
		})
	}

	return nil
}
