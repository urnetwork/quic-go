package quic

// urnetwork: a canceled dial closes with a CONNECTION_CLOSE (URNETWORK.md, "A
// canceled dial closes"). The server sees the client's close, not its own
// handshake idle timeout, and the cancellation never waits on the write.

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The server's handshake idle timeout in these tests: far longer than any
// step waits, so a server that ends its handshake within a step heard the
// client's close.
const canceledDialTestServerIdleTimeout = 60 * time.Second

// Forwards every datagram the client sends to the server and drops every one
// the server sends back, so the server holds a handshake the client can never
// complete.
func newCanceledDialTestOneWayRelay(t *testing.T, server net.Addr) *net.UDPConn {
	t.Helper()
	clientSide := connGroupTestSocket(t)
	upstream := connGroupTestSocket(t)
	go func() {
		b := make([]byte, 2048)
		for {
			n, _, err := clientSide.ReadFrom(b)
			if err != nil {
				return
			}
			upstream.WriteTo(b[:n], server)
		}
	}()
	return clientSide
}

// The connection close a server conn ended with once its client canceled the
// dial, within one test step.
func canceledDialTestServerClose(t *testing.T, serverConn *Conn) error {
	t.Helper()
	select {
	case <-serverConn.Context().Done():
		return context.Cause(serverConn.Context())
	case <-time.After(connGroupTestTimeout):
		t.Fatal("the server saw no close: its handshake waits out the idle timeout")
		return nil
	}
}

// A dial canceled while the server holds its handshake ends the server's
// connection with the client's close, an APPLICATION_ERROR transport close in
// the Initial packet space, which the server records as remote.
func TestUrnetworkCanceledDialSendsConnectionClose(t *testing.T) {
	serverTls, clientTls := connGroupTestTls(t)
	serverTransport := &Transport{Conn: connGroupTestSocket(t)}
	t.Cleanup(func() { serverTransport.Close() })
	listener, err := serverTransport.ListenEarly(serverTls, &Config{
		HandshakeIdleTimeout: canceledDialTestServerIdleTimeout,
	})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	relay := newCanceledDialTestOneWayRelay(t, serverTransport.Conn.LocalAddr())

	clientTransport := &Transport{Conn: connGroupTestSocket(t)}
	t.Cleanup(func() { clientTransport.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dialErrs := make(chan error, 1)
	go func() {
		_, err := clientTransport.Dial(ctx, relay.LocalAddr(), clientTls, &Config{
			HandshakeIdleTimeout: canceledDialTestServerIdleTimeout,
		})
		dialErrs <- err
	}()

	// an early connection is accepted once the server has processed the
	// ClientHello, so the server holds this handshake now
	acceptCtx, acceptCancel := context.WithTimeout(context.Background(), connGroupTestTimeout)
	defer acceptCancel()
	serverConn, err := listener.Accept(acceptCtx)
	require.NoError(t, err)

	cancel()
	select {
	case err := <-dialErrs:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(connGroupTestTimeout):
		t.Fatal("the canceled dial did not return")
	}

	closeErr := canceledDialTestServerClose(t, serverConn)
	var idleTimeoutErr *IdleTimeoutError
	require.False(t, errors.As(closeErr, &idleTimeoutErr), "the server timed out: %v", closeErr)
	var transportErr *TransportError
	require.ErrorAs(t, closeErr, &transportErr)
	require.True(t, transportErr.Remote)
	require.Equal(t, ApplicationErrorErrorCode, transportErr.ErrorCode)
}

// A packet conn whose writes block, once armed, until released.
type canceledDialTestBlockingConn struct {
	net.PacketConn
	armed       chan struct{}
	blocked     chan struct{}
	blockedOnce sync.Once
	release     chan struct{}
}

func newCanceledDialTestBlockingConn(t *testing.T) *canceledDialTestBlockingConn {
	return &canceledDialTestBlockingConn{
		PacketConn: connGroupTestSocket(t),
		armed:      make(chan struct{}),
		blocked:    make(chan struct{}),
		release:    make(chan struct{}),
	}
}

func (c *canceledDialTestBlockingConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	select {
	case <-c.armed:
		c.blockedOnce.Do(func() { close(c.blocked) })
		<-c.release
	default:
	}
	return c.PacketConn.WriteTo(b, addr)
}

func (c *canceledDialTestBlockingConn) releaseWrites() {
	select {
	case <-c.release:
	default:
		close(c.release)
	}
}

// The cancellation returns while the socket write of the close is still
// blocked, and the server sees the close once the socket takes it.
func TestUrnetworkCanceledDialReturnsWhileTheCloseWriteBlocks(t *testing.T) {
	serverTls, clientTls := connGroupTestTls(t)
	serverTransport := &Transport{Conn: connGroupTestSocket(t)}
	t.Cleanup(func() { serverTransport.Close() })
	listener, err := serverTransport.ListenEarly(serverTls, &Config{
		HandshakeIdleTimeout: canceledDialTestServerIdleTimeout,
	})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	relay := newCanceledDialTestOneWayRelay(t, serverTransport.Conn.LocalAddr())

	clientConn := newCanceledDialTestBlockingConn(t)
	clientTransport := &Transport{Conn: clientConn}
	// Transport.Close waits for the run loop, so a test that fails before
	// releasing the writes releases them first
	t.Cleanup(func() {
		clientConn.releaseWrites()
		clientTransport.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dialErrs := make(chan error, 1)
	go func() {
		_, err := clientTransport.Dial(ctx, relay.LocalAddr(), clientTls, &Config{
			HandshakeIdleTimeout: canceledDialTestServerIdleTimeout,
		})
		dialErrs <- err
	}()
	acceptCtx, acceptCancel := context.WithTimeout(context.Background(), connGroupTestTimeout)
	defer acceptCancel()
	serverConn, err := listener.Accept(acceptCtx)
	require.NoError(t, err)

	// every write from here on blocks until released: a retransmission the
	// send queue must flush, or the close itself
	close(clientConn.armed)
	cancel()
	select {
	case err := <-dialErrs:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(connGroupTestTimeout):
		t.Fatal("the canceled dial waited on a blocked socket write")
	}
	select {
	case <-clientConn.blocked:
	case <-time.After(connGroupTestTimeout):
		t.Fatal("the canceled dial wrote no close")
	}
	require.NoError(t, serverConn.Context().Err(), "the close passed a blocked write")

	clientConn.releaseWrites()
	closeErr := canceledDialTestServerClose(t, serverConn)
	var transportErr *TransportError
	require.ErrorAs(t, closeErr, &transportErr)
	require.True(t, transportErr.Remote)
	require.Equal(t, ApplicationErrorErrorCode, transportErr.ErrorCode)
}
