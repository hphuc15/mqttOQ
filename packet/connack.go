package packet

type ConnackPacket struct {
	FixedHeader

	SessionPresentFlag bool
	// ConnectReturnCode  ConnectionReturnCode
}
