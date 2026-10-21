package packet

import "errors"

type FixedHeader struct {
	// byte 1
	Type            byte
	Dup             bool  // Duplicate delivery of a PUBLISH Control Packet
	Qos             byte  // PUBLISH Quality of Service
	Retain          bool  // PUBLISH Retain flag
	RemainingLength int32 // The Remaining Length is the number of bytes remaining within the current packet.
}

const maxRemainingLength = 268435455

func (f *FixedHeader) encodeFixedHeader(remainingLength int) ([]byte, error) {
	if f.Type > 15 {
		return nil, errors.New("invalid control packet type")
	}
	if f.Qos > 2 {
		return nil, errors.New("invald qos")
	}
	var fixedHeader []byte
	remainingLenghOutput, e := encodeRemainingLength(remainingLength)
	if e != nil {
		return nil, e
	}
	fixedHeader = append(fixedHeader, f.Type<<4|flagMask(f.Dup)<<3|f.Qos<<1|flagMask(f.Retain))
	fixedHeader = append(fixedHeader, remainingLenghOutput...)
	return fixedHeader, nil
}
