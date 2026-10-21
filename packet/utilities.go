package packet

import (
	"encoding/binary"
	"fmt"
)

func encodeBytes(field []byte) []byte {
	// Attempting to encode more than 65,535 bytes would lead to an unexpected 16-bit length and extra data written
	// (which would be parsed as later parts of the message). The safest option is to truncate.
	if len(field) > 65535 {
		field = field[0:65535]
	}
	fieldLength := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldLength, uint16(len(field)))
	return append(fieldLength, field...)
}

func encodeString(s string) []byte {
	return encodeBytes([]byte(s))
}

func encodeUint16(n uint16) []byte {
	uint16Bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(uint16Bytes, n)
	return uint16Bytes
}

func flagMask(f bool) byte {
	if f {
		return 1
	}
	return 0
}

func encodeRemainingLength(l int) ([]byte, error) {
	if l < 0 || l > 268435455 {
		return nil, fmt.Errorf("invalid remaining length: %d", l)
	}

	var lengthBytes []byte
	for {
		b := byte(l % 128)
		l /= 128
		if l > 0 {
			b |= 0x80
		}
		lengthBytes = append(lengthBytes, b)
		if l == 0 {
			return lengthBytes, nil
		}
	}
}

// type utf8estring struct {
// 	StringLength  uint16
// 	CharacterData string
// }

// func ToUTF8String(s string) ([]byte, error) {
// 	if len(s) > 65535 {
// 		return nil, fmt.Errorf("Exceeds 65535 bytes.")
// 	}
// 	if !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
// 		return nil, errors.New("String is not valid MQTT UTF-8")
// 	}

// 	out := make([]byte, 2+len(s))
// 	binary.BigEndian.PutUint16(out, uint16(len(s)))
// 	copy(out[2:], s)

// 	return out, nil
// }

// func writeBytes(buf *bytes.Buffer, b []byte) error {
// 	if len(b) > 65535 {
// 		return fmt.Errorf("data exceeds 65535 bytes: %d", len(b))
// 	}
// 	buf.WriteByte(byte(len(b) >> 8))
// 	buf.WriteByte(byte(len(b)))
// 	buf.Write(b)
// 	return nil
// }

// func writeUTF8String(buf *bytes.Buffer, s string) error {
// 	stringLength := len(s)
// 	if stringLength > 65535 {
// 		return fmt.Errorf("Exceeds 65535 byte.")
// 	}

// 	buf.WriteByte(byte(stringLength >> 8))
// 	buf.WriteByte(byte(stringLength))
// 	buf.Write([]byte(s))
// 	return nil
// }

// func encodeBytes(field []byte) []byte {
// 	// Attempting to encode more than 65,535 bytes would lead to an unexpected 16-bit length and extra data written
// 	// (which would be parsed as later parts of the message). The safest option is to truncate.
// 	if len(field) > 65535 {
// 		field = field[0:65535]
// 	}
// 	fieldLength := make([]byte, 2)
// 	binary.BigEndian.PutUint16(fieldLength, uint16(len(field)))
// 	return append(fieldLength, field...)
// }
