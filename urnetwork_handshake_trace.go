package quic

import (
	"github.com/urnetwork/quic-go/internal/ackhandler"
	"github.com/urnetwork/quic-go/internal/handshake"
	"github.com/urnetwork/quic-go/qlogwriter"
)

// A HandshakeOnlyTrace is a qlogwriter.Trace that records a connection's
// events only until its handshake completes. Config.Tracer returns one for a
// connection whose handshake alone it measures, such as a server that
// classifies the handshakes that fail.
//
// quic-go builds each event of a traced connection before it hands the event
// to a recorder, so a trace that ignored an established connection's events
// would still cost their construction, a few allocations a packet, for the
// connection's whole life. Once the handshake completes, the connection asks
// HandshakeOnly, on its run loop. When it reports true, the connection and
// its loss recovery, congestion control, ECN tracking, TLS key updates and
// path MTU discovery drop their recorder, so none of them builds an event
// from then on, and the recorder is closed then, rather than when the
// connection closes. The connection then records no ConnectionClosed event.
// An http3 producer, which http3 adds only to a trace that supports its
// schema, is http3's and records on.
//
// A trace that doesn't implement HandshakeOnlyTrace, or whose HandshakeOnly
// reports false, records the connection's whole life, as before.
type HandshakeOnlyTrace interface {
	qlogwriter.Trace
	// whether the connection stops recording once its handshake completes
	HandshakeOnly() bool
}

// Ends the recording of a connection whose trace records only its handshake,
// once the handshake completed. Runs on the run loop, which owns every
// recorder of the connection but its own: handlePacket reads that one on the
// transport's goroutine, under receivedPacketMx, and records under it, so it
// is closed and dropped under receivedPacketMx too.
func (c *Conn) endHandshakeOnlyTrace() {
	if c.qlogger == nil {
		return
	}
	trace, ok := c.qlogTrace.(HandshakeOnlyTrace)
	if !ok || !trace.HandshakeOnly() {
		return
	}
	ackhandler.StopRecording(c.sentPacketHandler)
	if cryptoSetup, ok := c.cryptoStreamHandler.(handshake.CryptoSetup); ok {
		handshake.StopRecording(cryptoSetup)
	}
	if mtuFinder, ok := c.mtuDiscoverer.(*mtuFinder); ok {
		mtuFinder.qlogger = nil
	}
	c.receivedPacketMx.Lock()
	defer c.receivedPacketMx.Unlock()
	c.qlogger.Close()
	c.qlogger = nil
}
