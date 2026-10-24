/**
 * Ref: Spec MQTT 3.1.1.
 * Link: https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180843
 */
package packet

import (
	"errors"
	"io"
)

// ConnackReturnCode represents the 1-byte return code in CONNACK.
type ConnackReturnCode byte

const (
	Accepted                  ConnackReturnCode = 0x00 // Connection Accepted.
	RefusedProtocolVersion    ConnackReturnCode = 0x01 // The Server does not support the level of the MQTT protocol requested by the Client.
	RefusedIdentifierRejected ConnackReturnCode = 0x02 // The Client identifier is correct UTF-8 but not allowed by the Server.
	RefusedServerUnavailable  ConnackReturnCode = 0x03 // The Network Connection has been made but the MQTT service is unavailable.
	RefusedBadCredentials     ConnackReturnCode = 0x04 // The data in the user name or password is malformed.
	RefusedNotAuthorized      ConnackReturnCode = 0x05 // The Client is not authorized to connect.
)

// String returns human-readable description of return code.
func (rc ConnackReturnCode) String() string {
	switch rc {
	case Accepted:
		return "Connection Accepted"
	case RefusedProtocolVersion:
		return "Connection Refused, unacceptable protocol version"
	case RefusedIdentifierRejected:
		return "Connection Refused, identifier rejected"
	case RefusedServerUnavailable:
		return "Connection Refused, server unavailable"
	case RefusedBadCredentials:
		return "Connection Refused, bad user name or password"
	case RefusedNotAuthorized:
		return "Connection Refused, not authorized"
	default:
		return "Connection Refused, reserved code"
	}
}

type ConnectAckFlags struct{ SessionPresent bool }

// Body (Variable Header + Payload) of CONNACK Control Packet
type ConnackPacket struct {
	AckFlags   ConnectAckFlags
	ReturnCode ConnackReturnCode
}

// Type returns the CONNACK packet type.
func (p *ConnackPacket) Type() ControlPacketType {
	return CONNACK
}

// Flags returns the CONNACK flags (always 0).
func (p *ConnackPacket) Flags() byte {
	return 0
}

// Encode writes the CONNACK packet to the writer.
func (p *ConnackPacket) Encode(w io.Writer) error {
	var e error

	e = EncodeFixedHeader(w, CONNACK, 0, 2)
	if e != nil {
		return e
	}

	e = writeUint8(w, flagBoolToBitMask(p.AckFlags.SessionPresent, 0))
	if e != nil {
		return e
	}

	e = writeUint8(w, byte(p.ReturnCode))
	if e != nil {
		return e
	}

	return nil
}

// Decode reads the CONNACK packet variable header.
func (p *ConnackPacket) Decode(r io.Reader, remainLength uint32) error {
	if remainLength != 2 {
		return errors.New("malformed CONNACK packet: remaining length must be 2")
	}

	var b [2]byte
	_, e := io.ReadFull(r, b[:])
	if e != nil {
		return e
	}

	p.AckFlags.SessionPresent = (b[0] & 0x01) != 0
	p.ReturnCode = ConnackReturnCode(b[1])
	return nil
}
