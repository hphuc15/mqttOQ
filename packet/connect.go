/**
 * Ref: Spec MQTT 3.1.1.
 * Link: https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180843
 */
package packet

import (
	"bytes"
	"fmt"
	"io"
)

// Body (Variable Header + Payload) of CONNECT Control Packet
type ConnectPacket struct {
	ProtocolName     string
	ProtocolLevel    byte
	UserNameFlag     bool
	PasswordFlag     bool
	WillRetain       bool
	WillQos          byte
	WillFlag         bool
	CleanSession     bool
	KeepAlive        uint16
	ClientIdentifier string
	WillTopic        string
	WillMessage      []byte
	UserName         string
	Password         []byte
}

func (p *ConnectPacket) Type() ControlPacketType {
	return CONNECT
}

func (p *ConnectPacket) Flags() byte {
	return 0
}

func (p *ConnectPacket) Encode(w io.Writer) error {
	var body bytes.Buffer
	var e error

	/* Validate */
	if p.WillQos > 2 {
		/* If the Will Flag is set to 1, the value of Will QoS can be 0 (0x00), 1 (0x01), or 2 (0x02). It MUST NOT be 3 (0x03) [MQTT-3.1.2-14]. */
		return fmt.Errorf("invalid CONNECT: will QoS %d", p.WillQos)
	}
	if !p.WillFlag && (p.WillQos != 0 || p.WillRetain) {
		/**
		 * [MQTT-3.1.2-13] If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00).
		 * [MQTT-3.1.2-15] If the Will Flag is set to 0, then the Will Retain Flag MUST be set to 0.
		 */
		return fmt.Errorf("invalid CONNECT: will QoS/retain set without will flag")
	}
	if !p.UserNameFlag && p.PasswordFlag {
		/** [MQTT-3.1.2-22] If the User Name Flag is set to 0, the Password Flag MUST be set to 0. */
		return fmt.Errorf("invalid CONNECT: password flag set without username flag")
	}

	/* Variable Header: Protocol Name */
	e = encodeString(&body, p.ProtocolName)
	if e != nil {
		return e
	}

	/* Variable Header: Protocol Level */
	e = writeUint8(&body, p.ProtocolLevel)
	if e != nil {
		return e
	}

	/* Variable Header: Flags */
	flagByte := byte(flagBoolToBitMask(p.UserNameFlag, 7) | flagBoolToBitMask(p.PasswordFlag, 6) | flagBoolToBitMask(p.WillRetain, 5) | p.WillQos<<3 | flagBoolToBitMask(p.WillFlag, 2) | flagBoolToBitMask(p.CleanSession, 1))
	e = writeUint8(&body, flagByte)
	if e != nil {
		return e
	}

	/* Variable Header: Keep Alive */
	e = writeUint16(&body, p.KeepAlive)
	if e != nil {
		return e
	}

	/* Payload: Client Identifier */
	e = encodeString(&body, p.ClientIdentifier)
	if e != nil {
		return e
	}

	/* Payload: Will Topic */
	if p.WillFlag {
		// Will Topic
		e = encodeString(&body, p.WillTopic)
		if e != nil {
			return e
		}
		// Will Message
		e = encodeBytes(&body, p.WillMessage)
		if e != nil {
			return e
		}
	}

	/* Payload: User Name */
	if p.UserNameFlag {
		e = encodeString(&body, p.UserName)
		if e != nil {
			return e
		}
	}

	/* Payload: Password */
	if p.PasswordFlag {
		e = encodeBytes(&body, p.Password)
		if e != nil {
			return e
		}
	}

	remainLength := body.Len()
	e = EncodeFixedHeader(w, CONNECT, p.Flags(), uint32(remainLength))
	if e != nil {
		return e
	}

	_, e = body.WriteTo(w)
	if e != nil {
		return e
	}

	return nil
}

func (p *ConnectPacket) Decode(r io.Reader, remainLength uint32) error {
	lr := &io.LimitedReader{
		R: r,
		N: int64(remainLength),
	}
	var e error
	p.ProtocolName, e = decodeString(lr)
	if e != nil {
		return e
	}
	if p.ProtocolName != "MQTT" {
		return fmt.Errorf("malformed CONNECT: invalid protocol name %q", p.ProtocolName)
	}

	p.ProtocolLevel, e = readByte(lr)
	if e != nil {
		return e
	}

	flagsByte, e := readByte(lr)
	if e != nil {
		return e
	}
	p.UserNameFlag = flagBitMaskToBool(flagsByte, 7)
	p.PasswordFlag = flagBitMaskToBool(flagsByte, 6)
	p.WillRetain = flagBitMaskToBool(flagsByte, 5)
	p.WillQos = (flagsByte >> 3) & 0x03
	p.WillFlag = flagBitMaskToBool(flagsByte, 2)
	p.CleanSession = flagBitMaskToBool(flagsByte, 1)
	reservedBit := flagBitMaskToBool(flagsByte, 0)
	if p.WillQos > 2 {
		/* Only QoS0, QoS1, QoS2 */
		return fmt.Errorf("malformed CONNECT: invalid will QoS %d", p.WillQos)
	}
	if !p.WillFlag {
		if (p.WillQos != 0x00) || p.WillRetain {
			/**
			 * [3.1.2.5]
			 * If the Will Flag is set to 0 the Will QoS and Will Retain fields in the Connect Flags MUST be set to zero and the Will Topic and Will Message fields MUST NOT be present in the payload [MQTT-3.1.2-11].
			 * If the Will Flag is set to 0, a Will Message MUST NOT be published when this Network Connection ends [MQTT-3.1.2-12].
			 */
			return fmt.Errorf("malformed CONNECT: will QoS/retain set without will flag")
		}
	}
	if !p.UserNameFlag {
		if p.PasswordFlag {
			/**
			 * If the User Name Flag is set to 0, the Password Flag MUST be set to 0 [MQTT-3.1.2-22].
			 */
			return fmt.Errorf("malformed CONNECT: password without username")
		}
	}
	if reservedBit {
		/**
		 * [MQTT-3.1.2-3].
		 * "The Server MUST validate that the reserved flag in the
		 * CONNECT Control Packet is set to zero and disconnect the
		 * Client if it is not zero."
		 */
		return fmt.Errorf("malformed CONNECT: reserved flag bit set")
	}

	p.KeepAlive, e = readUint16(lr)
	if e != nil {
		return e
	}

	p.ClientIdentifier, e = decodeString(lr)
	if e != nil {
		return e
	}

	if p.WillFlag {
		p.WillTopic, e = decodeString(lr)
		if e != nil {
			return e
		}

		p.WillMessage, e = decodeBytes(lr)
		if e != nil {
			return e
		}
	}

	if p.UserNameFlag {
		p.UserName, e = decodeString(lr)
		if e != nil {
			return e
		}
	}

	if p.PasswordFlag {
		p.Password, e = decodeBytes(lr)
		if e != nil {
			return e
		}
	}

	if lr.N != 0 {
		return fmt.Errorf("malformed CONNECT: %d unexpected trailing bytes", lr.N)
	}
	return nil
}
