package packet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

type SubackReturnCode byte

const (
	SuccessMaximumQoS0 SubackReturnCode = 0x00
	SuccessMaximumQoS1 SubackReturnCode = 0x01
	SuccessMaximumQoS2 SubackReturnCode = 0x02
	FailureSuback      SubackReturnCode = 0x80
)

// SubackPacket represents an MQTT SUBACK packet.
type SubackPacket struct {
	PacketIdentifier uint16
	ReturnCodes      []byte
}

func (p *SubackPacket) Type() ControlPacketType {
	return SUBACK
}

func (p *SubackPacket) Flags() byte {
	return 0
}

func (p *SubackPacket) Encode(w io.Writer) error {
	if p.PacketIdentifier == 0 {
		return errors.New("invalid SUBACK: zero packet identifier")
	}
	if len(p.ReturnCodes) == 0 {
		return errors.New("invalid SUBACK: no return codes")
	}

	var body bytes.Buffer
	var e error

	e = writeUint16(&body, p.PacketIdentifier)
	if e != nil {
		return e
	}

	_, e = body.Write(p.ReturnCodes)
	if e != nil {
		return e
	}

	e = EncodeFixedHeader(w, SUBACK, 0, uint32(body.Len()))
	if e != nil {
		return e
	}
	_, e = body.WriteTo(w)
	return e
}

func (p *SubackPacket) Decode(r io.Reader, remainLength uint32) error {
	if remainLength < 3 { // 2 bytes PacketIdentifier + at least 1 return code
		return errors.New("malformed SUBACK packet: remaining length too short")
	}

	lr := &io.LimitedReader{R: r, N: int64(remainLength)}

	var e error
	p.PacketIdentifier, e = readUint16(lr)
	if e != nil {
		return e
	}
	if p.PacketIdentifier == 0 {
		return errors.New("malformed SUBACK: zero packet identifier")
	}

	p.ReturnCodes = make([]byte, lr.N)
	_, e = io.ReadFull(lr, p.ReturnCodes)
	if e != nil {
		return e
	}

	for _, c := range p.ReturnCodes {
		switch SubackReturnCode(c) {
		case SuccessMaximumQoS0, SuccessMaximumQoS1, SuccessMaximumQoS2, FailureSuback:
		default:
			return fmt.Errorf("malformed SUBACK: invalid return code 0x%02X", c)
		}
	}
	return nil
}
