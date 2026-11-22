package quic

// urnetwork: a trace of the handshake only (URNETWORK.md, "A trace of the
// handshake only"), over real connections on loopback.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/quic-go/internal/ackhandler"
	"github.com/urnetwork/quic-go/internal/handshake"
	"github.com/urnetwork/quic-go/internal/monotime"
	"github.com/urnetwork/quic-go/internal/protocol"
	"github.com/urnetwork/quic-go/internal/testdata"
	"github.com/urnetwork/quic-go/internal/utils"
	"github.com/urnetwork/quic-go/internal/wire"
	"github.com/urnetwork/quic-go/qlog"
	"github.com/urnetwork/quic-go/qlogwriter"
)

// The bound on any one step of these tests; never waited out on success.
const handshakeTraceTestTimeout = 10 * time.Second

const handshakeTraceTestAlpn = "urnetwork-handshake-trace-test"

// A server tls config with a generated certificate, and a client config that
// trusts it.
func handshakeTraceTestTls(tb testing.TB) (*tls.Config, *tls.Config) {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "handshake-trace.example"},
		DNSNames:     []string{"handshake-trace.example"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		tb.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		tb.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos:   []string{handshakeTraceTestAlpn},
	}
	clientConfig := &tls.Config{
		RootCAs:    roots,
		ServerName: "handshake-trace.example",
		NextProtos: []string{handshakeTraceTestAlpn},
	}
	return serverConfig, clientConfig
}

// A trace of one connection that keeps every event it is given, and counts
// its closes and the events recorded after one.
type handshakeTraceTestTrace struct {
	stateLock        sync.Mutex
	events           []qlogwriter.Event
	closeCount       int
	eventsAfterClose int
}

// qlogwriter.Trace
func (self *handshakeTraceTestTrace) AddProducer() qlogwriter.Recorder {
	return self
}

// qlogwriter.Trace
func (self *handshakeTraceTestTrace) SupportsSchemas(schema string) bool {
	return schema == qlog.EventSchema
}

// qlogwriter.Recorder
func (self *handshakeTraceTestTrace) RecordEvent(event qlogwriter.Event) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if 0 < self.closeCount {
		self.eventsAfterClose += 1
	}
	self.events = append(self.events, event)
}

// qlogwriter.Recorder
func (self *handshakeTraceTestTrace) Close() error {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.closeCount += 1
	return nil
}

// The events so far, the closes and the events recorded after a close.
func (self *handshakeTraceTestTrace) snapshot() ([]qlogwriter.Event, int, int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return append([]qlogwriter.Event{}, self.events...), self.closeCount, self.eventsAfterClose
}

// A trace that keeps every event it is given while it records, and reports
// handshakeOnly as HandshakeOnly.
type handshakeTraceTestHandshakeOnlyTrace struct {
	*handshakeTraceTestTrace
	handshakeOnly bool
}

var _ HandshakeOnlyTrace = (*handshakeTraceTestHandshakeOnlyTrace)(nil)

// HandshakeOnlyTrace
func (self *handshakeTraceTestHandshakeOnlyTrace) HandshakeOnly() bool {
	return self.handshakeOnly
}

// A trace that only counts the events it is given, retaining none, so it
// allocates nothing of its own; handshakeOnly as countingHandshakeOnlyTrace.
type handshakeTraceTestCountingTrace struct {
	eventCount atomic.Int64
}

// qlogwriter.Trace
func (self *handshakeTraceTestCountingTrace) AddProducer() qlogwriter.Recorder {
	return self
}

// qlogwriter.Trace
func (self *handshakeTraceTestCountingTrace) SupportsSchemas(schema string) bool {
	return schema == qlog.EventSchema
}

// qlogwriter.Recorder
func (self *handshakeTraceTestCountingTrace) RecordEvent(qlogwriter.Event) {
	self.eventCount.Add(1)
}

// qlogwriter.Recorder
func (self *handshakeTraceTestCountingTrace) Close() error {
	return nil
}

// A counting trace of the handshake only.
type handshakeTraceTestCountingHandshakeOnlyTrace struct {
	handshakeTraceTestCountingTrace
}

// HandshakeOnlyTrace
func (self *handshakeTraceTestCountingHandshakeOnlyTrace) HandshakeOnly() bool {
	return true
}

// A client and a server on loopback, each traced by its trace when it has
// one, with the client's connection, a stream on it whose bytes the server
// reads, and the server's connection, both through their handshakes.
type handshakeTraceTestEnv struct {
	clientConn *Conn
	stream     *Stream
	serverConn *Conn
	// what the server read of the stream
	serverRead chan int64
}

// A config with trace as its tracer, or none for a nil trace.
func handshakeTraceTestConfig(trace qlogwriter.Trace) *Config {
	config := &Config{
		HandshakeIdleTimeout: handshakeTraceTestTimeout,
		MaxIdleTimeout:       time.Minute,
	}
	if trace != nil {
		config.Tracer = func(context.Context, bool, ConnectionID) qlogwriter.Trace {
			return trace
		}
	}
	return config
}

// Connects a client to a server, the server's connection traced by serverTrace
// and the client's by clientTrace, each when set. Everything closes with the
// test.
func newHandshakeTraceTestEnv(tb testing.TB, serverTrace qlogwriter.Trace, clientTrace qlogwriter.Trace) *handshakeTraceTestEnv {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), handshakeTraceTestTimeout)
	defer cancel()
	serverTls, clientTls := handshakeTraceTestTls(tb)
	server := &Transport{Conn: newUDPConnLocalhost(tb)}
	tb.Cleanup(func() { server.Close() })
	listener, err := server.Listen(serverTls, handshakeTraceTestConfig(serverTrace))
	if err != nil {
		tb.Fatal(err)
	}
	env := &handshakeTraceTestEnv{
		serverRead: make(chan int64, 1),
	}
	accepted := make(chan *Conn, 1)
	go func() {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			return
		}
		accepted <- conn
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		n, _ := io.Copy(io.Discard, stream)
		env.serverRead <- n
	}()
	client := &Transport{Conn: newUDPConnLocalhost(tb)}
	tb.Cleanup(func() { client.Close() })
	env.clientConn, err = client.Dial(ctx, server.Conn.LocalAddr(), clientTls, handshakeTraceTestConfig(clientTrace))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { env.clientConn.CloseWithError(0, "") })
	select {
	case env.serverConn = <-accepted:
	case <-ctx.Done():
		tb.Fatal("the server accepted no connection")
	}
	select {
	case <-env.serverConn.HandshakeComplete():
	case <-ctx.Done():
		tb.Fatal("the server's handshake never completed")
	}
	env.stream, err = env.clientConn.OpenStreamSync(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	return env
}

// Writes count messages of size bytes on the client's stream, closes it, and
// waits for the server to have read them all.
func (env *handshakeTraceTestEnv) send(tb testing.TB, count int, size int) {
	tb.Helper()
	message := make([]byte, size)
	env.stream.SetWriteDeadline(time.Now().Add(handshakeTraceTestTimeout))
	for range count {
		if _, err := env.stream.Write(message); err != nil {
			tb.Fatal(err)
		}
	}
	env.stream.Close()
	select {
	case n := <-env.serverRead:
		if n != int64(count*size) {
			tb.Fatalf("the server read %d bytes, want %d", n, count*size)
		}
	case <-time.After(handshakeTraceTestTimeout):
		tb.Fatal("the server never read the stream")
	}
}

// Closes the client's connection and waits for the server's to end.
func (env *handshakeTraceTestEnv) close(tb testing.TB) {
	tb.Helper()
	env.clientConn.CloseWithError(0, "")
	select {
	case <-env.serverConn.Context().Done():
	case <-time.After(handshakeTraceTestTimeout):
		tb.Fatal("the server's connection never ended")
	}
}

// Whether events hold a packet of packetType, sent or received.
func handshakeTraceTestHasPacket(events []qlogwriter.Event, sent bool, packetType qlog.PacketType) bool {
	for _, event := range events {
		switch event := event.(type) {
		case qlog.PacketSent:
			if sent && event.Header.PacketType == packetType {
				return true
			}
		case qlog.PacketReceived:
			if !sent && event.Header.PacketType == packetType {
				return true
			}
		}
	}
	return false
}

// A trace of the handshake only records each side's handshake, then nothing:
// it is closed once, when the handshake completes, before HandshakeComplete
// returns, and the connection's later packets, its path MTU discovery and its
// close record no event, on the server and on the client.
func TestUrnetworkHandshakeOnlyTraceEndsAtHandshakeCompletion(t *testing.T) {
	serverTrace := &handshakeTraceTestHandshakeOnlyTrace{handshakeTraceTestTrace: &handshakeTraceTestTrace{}, handshakeOnly: true}
	clientTrace := &handshakeTraceTestHandshakeOnlyTrace{handshakeTraceTestTrace: &handshakeTraceTestTrace{}, handshakeOnly: true}
	env := newHandshakeTraceTestEnv(t, serverTrace, clientTrace)

	traces := []struct {
		side  string
		trace *handshakeTraceTestHandshakeOnlyTrace
	}{
		{side: "server", trace: serverTrace},
		{side: "client", trace: clientTrace},
	}
	handshakeEventCounts := map[string]int{}
	for _, c := range traces {
		events, closeCount, _ := c.trace.snapshot()
		if closeCount != 1 {
			t.Fatalf("the %s's trace was closed %d times once its handshake completed, want 1", c.side, closeCount)
		}
		if !handshakeTraceTestHasPacket(events, true, qlog.PacketTypeInitial) || !handshakeTraceTestHasPacket(events, false, qlog.PacketTypeHandshake) {
			t.Fatalf("the %s's trace has no handshake: %d events", c.side, len(events))
		}
		handshakeEventCounts[c.side] = len(events)
	}

	env.send(t, 100, 1000)
	env.close(t)
	for _, c := range traces {
		events, closeCount, eventsAfterClose := c.trace.snapshot()
		if len(events) != handshakeEventCounts[c.side] || eventsAfterClose != 0 || closeCount != 1 {
			t.Fatalf("after the handshake the %s's trace got %d more events, %d after its close, and %d closes", c.side, len(events)-handshakeEventCounts[c.side], eventsAfterClose, closeCount)
		}
	}
}

// A trace that doesn't opt in, by not implementing HandshakeOnlyTrace or by
// reporting false, records the connection's whole life, as before: packets
// after the handshake, RTT samples, the close, and an event for every packet
// the connection sent; it is closed once, when the connection closes.
func TestUrnetworkTraceThatDoesNotOptInRecordsEveryEvent(t *testing.T) {
	cases := []struct {
		what  string
		trace func() (qlogwriter.Trace, *handshakeTraceTestTrace)
	}{
		{
			what: "a trace without HandshakeOnly",
			trace: func() (qlogwriter.Trace, *handshakeTraceTestTrace) {
				trace := &handshakeTraceTestTrace{}
				return trace, trace
			},
		},
		{
			what: "a trace whose HandshakeOnly is false",
			trace: func() (qlogwriter.Trace, *handshakeTraceTestTrace) {
				trace := &handshakeTraceTestTrace{}
				return &handshakeTraceTestHandshakeOnlyTrace{handshakeTraceTestTrace: trace}, trace
			},
		},
	}
	for _, c := range cases {
		serverTrace, recorder := c.trace()
		env := newHandshakeTraceTestEnv(t, serverTrace, nil)
		if _, closeCount, _ := recorder.snapshot(); closeCount != 0 {
			t.Fatalf("%s: closed at the handshake", c.what)
		}
		env.send(t, 100, 1000)
		env.close(t)

		events, closeCount, eventsAfterClose := recorder.snapshot()
		if closeCount != 1 || eventsAfterClose != 0 {
			t.Fatalf("%s: %d closes, %d events after one", c.what, closeCount, eventsAfterClose)
		}
		if !handshakeTraceTestHasPacket(events, true, qlog.PacketType1RTT) || !handshakeTraceTestHasPacket(events, false, qlog.PacketType1RTT) {
			t.Fatalf("%s: no 1-RTT packet after the handshake", c.what)
		}
		sentEventCount := uint64(0)
		rttSampled := false
		closed := false
		for _, event := range events {
			switch event := event.(type) {
			case qlog.PacketSent:
				sentEventCount += 1
				for _, frame := range event.Frames {
					if _, ok := frame.Frame.(*qlog.ConnectionCloseFrame); ok {
						// a close is sent untracked
						sentEventCount -= 1
					}
				}
			case qlog.MetricsUpdated:
				rttSampled = rttSampled || event.LatestRTT != 0
			case qlog.ConnectionClosed:
				closed = true
			}
		}
		if !rttSampled || !closed {
			t.Fatalf("%s: an RTT sample %t, the close %t", c.what, rttSampled, closed)
		}
		if packetsSent := env.serverConn.ConnectionStats().PacketsSent; sentEventCount != packetsSent {
			t.Fatalf("%s: %d sent packets recorded, of %d sent", c.what, sentEventCount, packetsSent)
		}
	}
}

// Once a handshake completes, a connection whose trace records only the
// handshake drops the recorder of every part that holds one, the path MTU
// discoverer's too, which the connections on loopback may never exercise,
// and closes it once: its loss recovery and its keys then record nothing. A
// trace that doesn't opt in, or reports false, keeps every recorder, open.
func TestUrnetworkEndHandshakeOnlyTraceDropsEveryRecorder(t *testing.T) {
	newConn := func(trace qlogwriter.Trace, recorder qlogwriter.Recorder) (*Conn, *mtuFinder) {
		rttStats := utils.NewRTTStats()
		finder := newMTUDiscoverer(rttStats, protocol.InitialPacketSize, protocol.MaxPacketBufferSize, recorder)
		return &Conn{
			qlogTrace:     trace,
			qlogger:       recorder,
			mtuDiscoverer: finder,
			sentPacketHandler: ackhandler.NewSentPacketHandler(
				0,
				protocol.InitialPacketSize,
				rttStats,
				&utils.ConnectionStats{},
				false,
				false,
				nil,
				protocol.PerspectiveServer,
				recorder,
				utils.DefaultLogger,
			),
			cryptoStreamHandler: handshake.NewCryptoSetupServer(
				protocol.ParseConnectionID([]byte{1, 2, 3, 4, 5, 6, 7, 8}),
				&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4433},
				&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4434},
				&wire.TransportParameters{},
				testdata.GetTLSConfig(),
				false,
				rttStats,
				recorder,
				utils.DefaultLogger,
				protocol.Version1,
			),
		}, finder
	}
	// what the connection's loss recovery and keys record of a sent and
	// acknowledged packet and a change of Initial keys
	exercise := func(conn *Conn) {
		now := monotime.Now()
		pn := conn.sentPacketHandler.PopPacketNumber(protocol.Encryption1RTT)
		conn.sentPacketHandler.SentPacket(now, pn, protocol.InvalidPacketNumber, nil, []ackhandler.Frame{{Frame: &wire.PingFrame{}}}, protocol.Encryption1RTT, protocol.ECNNon, 1200, false, false)
		if _, err := conn.sentPacketHandler.ReceivedAck(&wire.AckFrame{AckRanges: []wire.AckRange{{Smallest: pn, Largest: pn}}}, protocol.Encryption1RTT, now.Add(time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		conn.cryptoStreamHandler.(handshake.CryptoSetup).ChangeConnectionID(protocol.ParseConnectionID([]byte{8, 7, 6, 5, 4, 3, 2, 1}))
	}

	trace := &handshakeTraceTestHandshakeOnlyTrace{handshakeTraceTestTrace: &handshakeTraceTestTrace{}, handshakeOnly: true}
	conn, finder := newConn(trace, trace.handshakeTraceTestTrace)
	conn.endHandshakeOnlyTrace()
	if conn.qlogger != nil || finder.qlogger != nil {
		t.Fatal("a recorder of the connection outlived its handshake-only trace")
	}
	eventsBefore, closeCount, _ := trace.snapshot()
	exercise(conn)
	conn.endHandshakeOnlyTrace()
	if events, _, _ := trace.snapshot(); len(events) != len(eventsBefore) || closeCount != 1 {
		t.Fatalf("after its end the trace got %d more events, and was closed %d times", len(events)-len(eventsBefore), closeCount)
	}

	for _, keep := range []struct {
		what  string
		trace func() (qlogwriter.Trace, *handshakeTraceTestTrace)
	}{
		{
			what: "a trace without HandshakeOnly",
			trace: func() (qlogwriter.Trace, *handshakeTraceTestTrace) {
				trace := &handshakeTraceTestTrace{}
				return trace, trace
			},
		},
		{
			what: "a trace whose HandshakeOnly is false",
			trace: func() (qlogwriter.Trace, *handshakeTraceTestTrace) {
				trace := &handshakeTraceTestTrace{}
				return &handshakeTraceTestHandshakeOnlyTrace{handshakeTraceTestTrace: trace}, trace
			},
		},
	} {
		trace, recorder := keep.trace()
		conn, finder := newConn(trace, recorder)
		conn.endHandshakeOnlyTrace()
		if conn.qlogger == nil || finder.qlogger == nil {
			t.Fatalf("%s: a recorder was dropped", keep.what)
		}
		eventsBefore, _, _ := recorder.snapshot()
		exercise(conn)
		if events, closeCount, _ := recorder.snapshot(); len(events) == len(eventsBefore) || closeCount != 0 {
			t.Fatalf("%s: %d more events, and %d closes", keep.what, len(events)-len(eventsBefore), closeCount)
		}
	}
}

// The cost of a trace after the handshake, on the server of a client
// streaming 1000-byte writes: untraced; traced; and traced for the handshake
// only, which costs what untraced does, since the connection builds no event
// once its handshake completes (trace-events/op 0). The trace only counts, so
// it allocates nothing itself.
func BenchmarkUrnetworkHandshakeOnlyTrace(b *testing.B) {
	run := func(b *testing.B, trace qlogwriter.Trace, eventCount func() int64) {
		env := newHandshakeTraceTestEnv(b, trace, nil)
		message := make([]byte, 1000)
		statsBefore := env.serverConn.ConnectionStats()
		eventsBefore := eventCount()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := env.stream.Write(message); err != nil {
				b.Fatal(err)
			}
		}
		env.stream.Close()
		<-env.serverRead
		b.StopTimer()
		statsAfter := env.serverConn.ConnectionStats()
		packets := float64(statsAfter.PacketsSent-statsBefore.PacketsSent) + float64(statsAfter.PacketsReceived-statsBefore.PacketsReceived)
		b.ReportMetric(packets/float64(b.N), "server-packets/op")
		b.ReportMetric(float64(eventCount()-eventsBefore)/float64(b.N), "trace-events/op")
		env.close(b)
	}
	b.Run("untraced", func(b *testing.B) {
		run(b, nil, func() int64 { return 0 })
	})
	b.Run("traced", func(b *testing.B) {
		trace := &handshakeTraceTestCountingTrace{}
		run(b, trace, trace.eventCount.Load)
	})
	b.Run("handshake-only", func(b *testing.B) {
		trace := &handshakeTraceTestCountingHandshakeOnlyTrace{}
		run(b, trace, trace.eventCount.Load)
	})
}
