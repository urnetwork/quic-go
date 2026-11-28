package wire

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urnetwork/quic-go/v2026/internal/protocol"
)

func TestWriteHandshakeDoneSampleFrame(t *testing.T) {
	frame := HandshakeDoneFrame{}
	b, err := frame.Append(nil, protocol.Version1)
	require.NoError(t, err)
	require.Equal(t, []byte{byte(FrameTypeHandshakeDone)}, b)
	require.Equal(t, protocol.ByteCount(1), frame.Length(protocol.Version1))
}
