package congestion

import (
	"testing"

	"github.com/urnetwork/quic-go/v2026/internal/protocol"
	"github.com/urnetwork/quic-go/v2026/internal/utils"
	"github.com/urnetwork/quic-go/v2026/testutils/events"

	"github.com/stretchr/testify/require"
)

// urnetwork: StopRecording drops a cubic sender's recorder, so it records no
// congestion state change from then on; any other SendAlgorithm is left as it
// is.
func TestUrnetworkCubicSenderStopRecording(t *testing.T) {
	var eventRecorder events.Recorder
	sender := NewCubicSender(DefaultClock{}, utils.NewRTTStats(), &utils.ConnectionStats{}, protocol.InitialPacketSize, true, &eventRecorder)
	require.NotEmpty(t, eventRecorder.Events())
	eventRecorder.Clear()

	StopRecording(sender)
	require.Nil(t, sender.qlogger)
	sender.OnCongestionEvent(1, protocol.InitialPacketSize, 2*protocol.InitialPacketSize)
	require.Empty(t, eventRecorder.Events())

	StopRecording(nil)
}
