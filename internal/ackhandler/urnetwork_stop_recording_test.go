package ackhandler

import (
	"testing"
	"time"

	"github.com/urnetwork/quic-go/v2026/internal/monotime"
	"github.com/urnetwork/quic-go/v2026/internal/protocol"
	"github.com/urnetwork/quic-go/v2026/internal/utils"
	"github.com/urnetwork/quic-go/v2026/internal/wire"
	"github.com/urnetwork/quic-go/v2026/testutils/events"

	"github.com/stretchr/testify/require"
)

// urnetwork: once StopRecording runs, the sent packet handler, its
// congestion controller, its ECN tracker and a congestion controller it builds
// after a path migration record nothing more, though the same sends and ACKs
// recorded before; a handler of another type is left as it is.
func TestUrnetworkSentPacketHandlerStopRecording(t *testing.T) {
	var eventRecorder events.Recorder
	sph := NewSentPacketHandler(
		0,
		1200,
		utils.NewRTTStats(),
		&utils.ConnectionStats{},
		false,
		true,
		nil,
		protocol.PerspectiveClient,
		&eventRecorder,
		utils.DefaultLogger,
	)
	// sends 1-RTT packets with ECN and acknowledges them
	exchange := func(now monotime.Time) {
		pns := []protocol.PacketNumber{}
		for range 3 {
			pn := sph.PopPacketNumber(protocol.Encryption1RTT)
			sph.SentPacket(now, pn, protocol.InvalidPacketNumber, nil, []Frame{{Frame: &wire.PingFrame{}}}, protocol.Encryption1RTT, protocol.ECT0, 1200, false, false)
			pns = append(pns, pn)
		}
		_, err := sph.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pns...), ECT0: 3}, protocol.Encryption1RTT, now.Add(10*time.Millisecond))
		require.NoError(t, err)
	}

	now := monotime.Now()
	exchange(now)
	require.NotEmpty(t, eventRecorder.Events())
	eventRecorder.Clear()

	StopRecording(sph)
	require.Nil(t, sph.(*sentPacketHandler).qlogger)
	require.Nil(t, sph.(*sentPacketHandler).ecnTracker.(*ecnTracker).qlogger)
	exchange(now.Add(20 * time.Millisecond))
	sph.MigratedPath(now.Add(30*time.Millisecond), 1200)
	exchange(now.Add(40 * time.Millisecond))
	require.Empty(t, eventRecorder.Events())

	StopRecording(nil)
}
