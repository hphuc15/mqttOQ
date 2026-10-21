/* Client requests a connection to a Server  */
package packet

import (
	"bytes"
	"errors"
	"fmt"
)

type ProtocolLevel byte

const (
	MqttVersion311 ProtocolLevel = 0x04
)

type QoS byte

const (
	QoS0 QoS = 0x00
	QoS1 QoS = 0x01
	QoS2 QoS = 0x02
)

type ConnectPacket struct {
	FixedHeader

	// ProtocolName  [6]byte
	ProtocolLevel byte
	UserNameFlag  bool
	PasswordFlag  bool
	WillRetain    bool
	WillQos       byte
	WillFlag      bool
	CleanSession  bool
	Reserved      bool
	KeepAlive     uint16

	ClientIdentifer string
	WillTopic       string
	WillMessage     []byte
	UserName        string
	Password        []byte
}

func (c *ConnectPacket) Encode() ([]byte, error) {
	e := c.validate()
	if e != nil {
		return nil, e
	}
	c.FixedHeader = FixedHeader{Type: byte(Connect)}

	var bodyBytes bytes.Buffer
	flagsByte := flagMask(c.UserNameFlag)<<7 | flagMask(c.PasswordFlag)<<6 | flagMask(c.WillRetain)<<5 | c.WillQos<<3 | flagMask(c.WillFlag)<<2 | flagMask(c.CleanSession)<<1

	/* Variable header */
	bodyBytes.Write([]byte{0x00, 0x04, 'M', 'Q', 'T', 'T'})
	bodyBytes.WriteByte(c.ProtocolLevel)
	bodyBytes.WriteByte(flagsByte)
	bodyBytes.Write(encodeUint16(c.KeepAlive))
	/* Payload */
	bodyBytes.Write(encodeString(c.ClientIdentifer))
	if c.WillFlag {
		bodyBytes.Write(encodeString(c.WillTopic))
		bodyBytes.Write(encodeBytes(c.WillMessage))
	}
	if c.UserNameFlag {
		bodyBytes.Write(encodeString(c.UserName))
	}
	if c.PasswordFlag {
		bodyBytes.Write(encodeBytes(c.Password))
	}

	fixedHeaderBytes, e := c.encodeFixedHeader(bodyBytes.Len())
	if e != nil {
		return nil, e
	}

	var connectPacketBytes bytes.Buffer
	connectPacketBytes.Write(fixedHeaderBytes)
	connectPacketBytes.Write(bodyBytes.Bytes())
	return connectPacketBytes.Bytes(), e
}

func (c *ConnectPacket) validate() error {
	if c.WillQos > 2 {
		return fmt.Errorf("invalid will QoS: %d", c.WillQos)
	}
	if c.WillFlag && c.WillTopic == "" {
		return errors.New("will flag set but will topic is empty")
	}
	if !c.WillFlag && (c.WillQos != 0 || c.WillRetain) {
		return errors.New("will QoS/retain set without will flag")
	}
	if c.PasswordFlag && !c.UserNameFlag {
		return errors.New("password flag requires username flag")
	}
	return nil
}
