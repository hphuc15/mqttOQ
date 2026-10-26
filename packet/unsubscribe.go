package packet

import (
	"bytes"
	"errors"
	"io"
)

type UnsubscribePacket struct {
	PacketIdentifier uint16
	TopicFilters     []string
}

func (p *UnsubscribePacket) Type() ControlPacketType {
	return UNSUBSCRIBE
}

func (p *UnsubscribePacket) Flags() byte {
	return 0x02
}

func (p *UnsubscribePacket) Encode(w io.Writer) error {
	if len(p.TopicFilters) == 0 {
		return errors.New("UNSUBSCRIBE packet must contain at least one topic")
	}

	var body bytes.Buffer
	e := writeUint16(&body, p.PacketIdentifier)
	if e != nil {
		return e
	}

	for _, topic := range p.TopicFilters {
		e = encodeString(&body, topic)
		if e != nil {
			return e
		}
	}

	e = EncodeFixedHeader(w, UNSUBSCRIBE, 0x02, uint32(body.Len()))
	if e != nil {
		return e
	}

	_, e = body.WriteTo(w)
	return e
}

func (p *UnsubscribePacket) Decode(r io.Reader, remainLength uint32) error {
	if remainLength < 4 { // 2 bytes PacketID + 2 bytes string len
		return errors.New("malformed UNSUBSCRIBE packet: remaining length too short")
	}
	lr := io.LimitReader(r, int64(remainLength))

	var e error
	p.PacketIdentifier, e = readUint16(lr)
	if e != nil {
		return e
	}

	bytesLeft := int(remainLength) - 2
	p.TopicFilters = nil

	for bytesLeft > 0 {
		topic, e := decodeString(lr)
		if e != nil {
			return e
		}
		p.TopicFilters = append(p.TopicFilters, topic)
		bytesLeft -= (len(topic) + 2)
	}

	return nil
}
