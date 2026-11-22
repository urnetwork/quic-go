package handshake

// Stops a crypto setup and its 1-RTT keys from recording key updates and
// discards, for a connection whose trace records only its handshake
// (quic.HandshakeOnlyTrace). Called on the connection's run loop, which owns
// them. A CryptoSetup this package didn't build is left as it is.
func StopRecording(cs CryptoSetup) {
	cryptoSetup, ok := cs.(*cryptoSetup)
	if !ok {
		return
	}
	cryptoSetup.qlogger = nil
	cryptoSetup.aead.qlogger = nil
}
