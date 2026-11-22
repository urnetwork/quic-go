package congestion

// Stops a cubic sender from recording its congestion state changes, for a
// connection whose trace records only its handshake (quic.HandshakeOnlyTrace).
// Called on the connection's run loop, which owns the sender. Any other
// SendAlgorithm is left as it is.
func StopRecording(sendAlgorithm SendAlgorithm) {
	if cubicSender, ok := sendAlgorithm.(*cubicSender); ok {
		cubicSender.qlogger = nil
	}
}
