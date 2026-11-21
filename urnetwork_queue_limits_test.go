package quic

import (
	"errors"
	"testing"
	"testing/synctest"

	"github.com/urnetwork/quic-go/internal/protocol"
	"github.com/urnetwork/quic-go/internal/utils"
	"github.com/urnetwork/quic-go/internal/wire"
	"github.com/urnetwork/quic-go/qlog"
	"github.com/urnetwork/quic-go/qlogwriter"
	"github.com/urnetwork/quic-go/testutils/events"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// A Config that leaves the queue limits unset gets upstream's lengths, set
// limits are kept, and an unprocessed-packet limit below a server's 0-RTT
// buffer is raised to hold it.
func TestUrnetworkConfigQueueLimits(t *testing.T) {
	c := populateConfig(&Config{})
	require.Equal(t, 256, c.MaxUnprocessedPackets)
	require.Equal(t, 32, c.MaxDatagramSendQueueLen)
	require.Equal(t, 128, c.MaxDatagramReceiveQueueLen)

	c = populateConfig(&Config{MaxUnprocessedPackets: -1, MaxDatagramSendQueueLen: -1, MaxDatagramReceiveQueueLen: -1})
	require.Equal(t, 256, c.MaxUnprocessedPackets)
	require.Equal(t, 32, c.MaxDatagramSendQueueLen)
	require.Equal(t, 128, c.MaxDatagramReceiveQueueLen)

	c = populateConfig(&Config{MaxUnprocessedPackets: 40, MaxDatagramSendQueueLen: 8, MaxDatagramReceiveQueueLen: 32})
	require.Equal(t, 40, c.MaxUnprocessedPackets)
	require.Equal(t, 8, c.MaxDatagramSendQueueLen)
	require.Equal(t, 32, c.MaxDatagramReceiveQueueLen)

	c = populateConfig(&Config{MaxUnprocessedPackets: 1})
	require.Equal(t, protocol.Max0RTTQueueLen+1, c.MaxUnprocessedPackets)
}

// A connection takes its DATAGRAM queue lengths from its Config.
func TestUrnetworkConnectionDatagramQueueLimits(t *testing.T) {
	tc := newServerTestConnection(t, nil, &Config{
		DisablePathMTUDiscovery:    true,
		EnableDatagrams:            true,
		MaxDatagramSendQueueLen:    8,
		MaxDatagramReceiveQueueLen: 32,
	}, false)
	require.Equal(t, 8, tc.conn.datagramQueue.sendMaxLen)
	require.Equal(t, 32, tc.conn.datagramQueue.rcvMaxLen)

	tc = newServerTestConnection(t, nil, &Config{DisablePathMTUDiscovery: true, EnableDatagrams: true}, false)
	require.Equal(t, maxDatagramSendQueueLen, tc.conn.datagramQueue.sendMaxLen)
	require.Equal(t, maxDatagramRcvQueueLen, tc.conn.datagramQueue.rcvMaxLen)
}

// The send queue holds its configured length and Add then blocks until a
// frame leaves; the receive queue holds its configured length and drops the
// rest.
func TestUrnetworkDatagramQueueConfiguredLengths(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := newDatagramQueueWithLimits(func() {}, utils.DefaultLogger, 3, 5)

		for range 3 {
			require.NoError(t, queue.Add(&wire.DatagramFrame{Data: []byte{0}}))
		}
		errChan := make(chan error, 1)
		go func() { errChan <- queue.Add(&wire.DatagramFrame{Data: []byte{1}}) }()
		synctest.Wait()
		select {
		case <-errChan:
			t.Fatal("a full send queue admitted another frame")
		default:
		}
		queue.Pop()
		synctest.Wait()
		select {
		case err := <-errChan:
			require.NoError(t, err)
		default:
			t.Fatal("a popped frame did not admit the waiting one")
		}

		for i := range 7 {
			queue.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte{byte(i)}})
		}
		for i := range 5 {
			data, err := queue.Receive(t.Context())
			require.NoError(t, err)
			require.Equal(t, []byte{byte(i)}, data)
		}
		closed := errors.New("closed")
		queue.CloseWithError(closed)
		_, err := queue.Receive(t.Context())
		require.ErrorIs(t, err, closed, "frames beyond the receive queue length were kept")
	})
}

// A connection drops a packet beyond its configured unprocessed-packet limit.
func TestUrnetworkConnectionMaxUnprocessedPackets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		var eventRecorder events.Recorder
		tc := newServerTestConnection(t, mockCtrl, &Config{
			DisablePathMTUDiscovery: true,
			MaxUnprocessedPackets:   40,
		}, false, connectionOptTracer(&eventRecorder))

		for range 40 {
			tc.conn.handlePacket(receivedPacket{data: []byte("foobar")})
		}
		synctest.Wait()
		require.Empty(t, eventRecorder.Events(qlog.PacketDropped{}))

		tc.conn.handlePacket(receivedPacket{data: []byte("foobar")})
		synctest.Wait()
		require.Equal(t,
			[]qlogwriter.Event{
				qlog.PacketDropped{
					Raw:     qlog.RawInfo{Length: 6},
					Trigger: qlog.PacketDropDOSPrevention,
				},
			},
			eventRecorder.Events(qlog.PacketDropped{}),
		)
	})
}
