package http3

// urnetwork: a server connection's idle timer in quic-go's census of timer
// changes (URNETWORK.md). Over a simulated network, under virtual time, a
// request stream that starts with no other active stops the pending idle
// timer, the end of the last active stream re-arms it for the idle timeout,
// and closing the connection stops it again: each is recorded once, the
// re-arm with the idle timeout as its horizon. A request that starts beside an
// active one finds the timer stopped already, and records nothing.

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/urnetwork/quic-go"
	"github.com/urnetwork/quic-go/testutils/simnet"

	"github.com/stretchr/testify/require"
)

// One recorded change.
type urnetworkTimerCensusRecord struct {
	site    quic.TimerCensusSite
	horizon time.Duration
	stop    bool
	count   int
}

// Records the changes that goroutines in a synctest bubble make. The census is
// process-wide, and a bubble's clock starts at 2000-01-01 UTC where every
// other goroutine's reads the real time, so a test counts only its own
// bubble's changes.
type urnetworkTimerCensusRecorder struct {
	mutex   sync.Mutex
	records []urnetworkTimerCensusRecord
}

// Before any real clock, after any bubble's.
var urnetworkTimerCensusBubbleEnd = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)

func (r *urnetworkTimerCensusRecorder) record(site quic.TimerCensusSite, horizon time.Duration, stop bool, count int) {
	if !time.Now().Before(urnetworkTimerCensusBubbleEnd) {
		return
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.records = append(r.records, urnetworkTimerCensusRecord{
		site:    site,
		horizon: horizon,
		stop:    stop,
		count:   count,
	})
}

// The bubble's changes at site, in order.
func (r *urnetworkTimerCensusRecorder) at(site quic.TimerCensusSite) []urnetworkTimerCensusRecord {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var records []urnetworkTimerCensusRecord
	for _, record := range r.records {
		if record.site == site {
			records = append(records, record)
		}
	}
	return records
}

func TestUrnetworkTimerCensusCountsTheIdleTimer(t *testing.T) {
	// the test certificates are valid from the real now, and the bubble's
	// clock reads 2000
	certificateTime := time.Now()
	synctest.Test(t, func(t *testing.T) {
		const idleTimeout = 47*time.Second + 13*time.Millisecond
		recorder := &urnetworkTimerCensusRecorder{}
		quic.SetTimerCensus(&quic.TimerCensus{
			SamplePeriod: 1,
			Record:       recorder.record,
		})
		t.Cleanup(func() {
			quic.SetTimerCensus(nil)
		})

		network := &simnet.Simnet{Router: &simnet.PerfectRouter{}}
		linkSettings := simnet.NodeBiDiLinkSettings{Latency: 5 * time.Millisecond}
		clientPacketConn := network.NewEndpoint(&net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 9001}, linkSettings)
		serverPacketConn := network.NewEndpoint(&net.UDPAddr{IP: net.ParseIP("192.0.2.2"), Port: 9002}, linkSettings)
		require.NoError(t, network.Start())
		defer func() {
			require.NoError(t, clientPacketConn.Close())
			require.NoError(t, serverPacketConn.Close())
			require.NoError(t, network.Close())
		}()

		ln, err := quic.ListenEarly(serverPacketConn, getTLSConfig(), &quic.Config{})
		require.NoError(t, err)
		defer ln.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		clientTlsConfig := getTLSClientConfig()
		clientTlsConfig.Time = func() time.Time {
			return certificateTime
		}
		clientConn, err := quic.DialEarly(ctx, clientPacketConn, serverPacketConn.LocalAddr(), clientTlsConfig, &quic.Config{})
		require.NoError(t, err)
		defer clientConn.CloseWithError(0, "")
		serverConn, err := ln.Accept(ctx)
		require.NoError(t, err)

		// a request to /held is held once its handler runs, until released
		held := make(chan struct{})
		release := make(chan struct{})
		server := &Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/held" {
					close(held)
					<-release
				}
				w.Write([]byte("synthetic"))
			}),
			IdleTimeout: idleTimeout,
		}
		served := make(chan error, 1)
		go func() {
			served <- server.ServeQUICConn(serverConn)
		}()

		transport := &Transport{}
		clientHttpConn := transport.NewClientConn(clientConn)
		get := func(path string) error {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://localhost"+path, nil)
			if err != nil {
				return err
			}
			response, err := clientHttpConn.RoundTrip(request)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			_, err = io.ReadAll(response.Body)
			return err
		}
		stop := urnetworkTimerCensusRecord{site: quic.TimerCensusSiteHTTP3IdleTimer, stop: true, count: 1}
		reset := urnetworkTimerCensusRecord{site: quic.TimerCensusSiteHTTP3IdleTimer, horizon: idleTimeout, count: 1}

		// one request: its start stops the timer, and its end, the connection's
		// last, re-arms it
		require.NoError(t, get("/"))
		synctest.Wait()
		require.Equal(t,
			[]urnetworkTimerCensusRecord{stop, reset},
			recorder.at(quic.TimerCensusSiteHTTP3IdleTimer),
		)

		// a request beside a held one: its start finds the timer stopped, and
		// its end leaves the held one active
		heldErr := make(chan error, 1)
		go func() {
			heldErr <- get("/held")
		}()
		<-held
		require.NoError(t, get("/"))
		synctest.Wait()
		require.Equal(t,
			[]urnetworkTimerCensusRecord{stop, reset, stop},
			recorder.at(quic.TimerCensusSiteHTTP3IdleTimer),
		)
		close(release)
		require.NoError(t, <-heldErr)
		synctest.Wait()
		require.Equal(t,
			[]urnetworkTimerCensusRecord{stop, reset, stop, reset},
			recorder.at(quic.TimerCensusSiteHTTP3IdleTimer),
		)

		// closing the connection stops the timer the last end re-armed
		require.NoError(t, server.Close())
		require.ErrorIs(t, <-served, http.ErrServerClosed)
		require.Equal(t,
			[]urnetworkTimerCensusRecord{stop, reset, stop, reset, stop},
			recorder.at(quic.TimerCensusSiteHTTP3IdleTimer),
		)
	})
}
