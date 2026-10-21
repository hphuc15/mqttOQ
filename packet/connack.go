package packet

import (
	"bytes"
	"errors"
)

type ConnackPacket struct {
	FixedHeader

	SessionPresentFlag bool
	ConnectReturnCode  byte
}

func (c *ConnackPacket) Encode() ([]byte, error) {
	c.FixedHeader = FixedHeader{
		Type:   byte(Connack),
		Dup:    false,
		Qos:    0,
		Retain: false,
	}

	var bodyBytes bytes.Buffer
	bodyBytes.WriteByte(flagMask(c.SessionPresentFlag))
	bodyBytes.WriteByte(byte(c.ConnectReturnCode))

	fixedHeaderBytes, e := c.encodeFixedHeader(bodyBytes.Len())
	if e != nil {
		return nil, e
	}

	var connackPacketBytes bytes.Buffer
	connackPacketBytes.Write(fixedHeaderBytes)
	connackPacketBytes.Write(bodyBytes.Bytes())
	return connackPacketBytes.Bytes(), nil
}

func (c *ConnackPacket) Decode(connackPacketBytes []byte) error {
	if connackPacketBytes[0] != byte(Connack) {
		return errors.New("invalid type")
	}

	c.Type = connackPacketBytes[0]
	c.Dup = false
	c.Qos = 0
	c.Retain = false
	c.RemainingLength = 2
	c.SessionPresentFlag = decodeFlagMask(connackPacketBytes[2], 0)
	c.ConnectReturnCode = connackPacketBytes[3]

	return nil
}
