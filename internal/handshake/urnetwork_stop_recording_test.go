package handshake

import (
	"net"
	"testing"

	"github.com/urnetwork/quic-go/internal/protocol"
	"github.com/urnetwork/quic-go/internal/testdata"
	"github.com/urnetwork/quic-go/internal/utils"
	"github.com/urnetwork/quic-go/internal/wire"
	"github.com/urnetwork/quic-go/testutils/events"

	"github.com/stretchr/testify/require"
)

// urnetwork: StopRecording drops the recorder of a crypto setup and of its
// 1-RTT keys, so a change of Initial keys, which it recorded before, records
// nothing; a CryptoSetup of another type is left as it is.
func TestUrnetworkCryptoSetupStopRecording(t *testing.T) {
	var eventRecorder events.Recorder
	cs := NewCryptoSetupServer(
		protocol.ParseConnectionID([]byte{1, 2, 3, 4, 5, 6, 7, 8}),
		&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4433},
		&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4434},
		&wire.TransportParameters{},
		testdata.GetTLSConfig(),
		false,
		utils.NewRTTStats(),
		&eventRecorder,
		utils.DefaultLogger,
		protocol.Version1,
	)
	cs.ChangeConnectionID(protocol.ParseConnectionID([]byte{8, 7, 6, 5, 4, 3, 2, 1}))
	require.NotEmpty(t, eventRecorder.Events())
	eventRecorder.Clear()

	StopRecording(cs)
	require.Nil(t, cs.(*cryptoSetup).qlogger)
	require.Nil(t, cs.(*cryptoSetup).aead.qlogger)
	cs.ChangeConnectionID(protocol.ParseConnectionID([]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	require.Empty(t, eventRecorder.Events())

	StopRecording(nil)
}
