package packet

import "fmt"

type ControlPacketType byte

const (
	Connect     ControlPacketType = 0x01 // Client request to connect to Server
	Connack     ControlPacketType = 0x02 // Connect acknowledgment
	Publish     ControlPacketType = 0x03 // Publish message
	Puback      ControlPacketType = 0x04 // Publish acknowledgment
	Pubrec      ControlPacketType = 0x05 // Publish received (assured delivery part 1)
	Pubrel      ControlPacketType = 0x06 // Publish release (assured delivery part 2)
	Pubcomp     ControlPacketType = 0x07 // Publish complete (assured delivery part 3)
	Subscribe   ControlPacketType = 0x08 // Client subscribe request
	Suback      ControlPacketType = 0x09 // Subscribe acknowledgment
	Unsubscribe ControlPacketType = 0x0A // Unsubscribe request
	Unsuback    ControlPacketType = 0x0B // Unsubscribe acknowledgment
	Pingreq     ControlPacketType = 0x0C // PING request
	Pingresp    ControlPacketType = 0x0D // PING response
	Disconnect  ControlPacketType = 0x0E // Client is disconnecting
)

type ConnectionReturnCode byte

const (
	ConnectionAccepted                  ConnectionReturnCode = 0x00 // Connection accepted
	ConnectionRefusedProtocolVersion    ConnectionReturnCode = 0x01 // The Server does not support the level of the MQTT protocol requested by the Client
	ConnectionRefusedIdentifierRejected ConnectionReturnCode = 0x02 // The Client identifier is correct UTF-8 but not allowed by the Server
	ConnectionRefusedServerUnavailable  ConnectionReturnCode = 0x03 // The Network Connection has been made but the MQTT service is unavailable
	ConnectionRefusedBadCredentials     ConnectionReturnCode = 0x04 // The data in the user name or password is malformed
	ConnectionRefusedNotAuthorized      ConnectionReturnCode = 0x05 // The Client is not authorized to connect
)

func (c ConnectionReturnCode) Error() string {
	switch c {
	case ConnectionAccepted:
		return "Connection accepted"
	case ConnectionRefusedProtocolVersion:
		return "Connection refused: The Server does not support the level of the MQTT protocol requested by the Client"
	case ConnectionRefusedIdentifierRejected:
		return "Connection refused: The Client identifier is correct UTF-8 but not allowed by the Server"
	case ConnectionRefusedServerUnavailable:
		return "Connection refused: The Network Connection has been made but the MQTT service is unavailable"
	case ConnectionRefusedBadCredentials:
		return "Connection refused: The data in the user name or password is malformed"
	case ConnectionRefusedNotAuthorized:
		return "Connection refused: The Client is not authorized to connect"
	default:
		return fmt.Sprintf("Connection refused: unknown return code %d", byte(c))
	}
}

func (c ConnectionReturnCode) Err() error {
	if c == ConnectionAccepted {
		return nil
	}
	return c
}
