package ackhandler

import "github.com/urnetwork/quic-go/internal/congestion"

// Stops h, its congestion controller and its ECN tracker from recording
// events, so none of them builds one from then on, for a connection whose
// trace records only its handshake (quic.HandshakeOnlyTrace). A congestion
// controller h builds later, after a path migration, takes h's recorder, which
// is then none. Called on the connection's run loop, which owns h. A
// SentPacketHandler this package didn't build, such as a test's mock, is left
// as it is.
func StopRecording(h SentPacketHandler) {
	sentPacketHandler, ok := h.(*sentPacketHandler)
	if !ok {
		return
	}
	sentPacketHandler.qlogger = nil
	if ecnTracker, ok := sentPacketHandler.ecnTracker.(*ecnTracker); ok {
		ecnTracker.qlogger = nil
	}
	congestion.StopRecording(sentPacketHandler.congestion)
}
