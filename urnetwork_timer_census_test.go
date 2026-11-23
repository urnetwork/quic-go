package quic

// urnetwork: the census of timer changes (URNETWORK.md). Under virtual time,
// each counted site records once per change it makes, with the time left to
// its deadline: a stream Read, Peek or Write each time its wait re-arms its
// deadline timer, and the stop of that timer when it is still pending as the
// call returns; and the connection's run loop timer only as resetTimer stops
// a pending timer. A timer that fired is never recorded as stopped. The census
// itself takes one change in its sample period, recorded as that many, and
// none once removed.

import (
	"context"
	"math"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/urnetwork/quic-go/internal/monotime"
	"github.com/urnetwork/quic-go/internal/protocol"
	"github.com/urnetwork/quic-go/internal/wire"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// One recorded change.
type urnetworkTimerCensusRecord struct {
	site    TimerCensusSite
	horizon time.Duration
	stop    bool
	count   int
	// made by a goroutine in a synctest bubble, whose clock starts at
	// 2000-01-01 UTC where every other goroutine's reads the real time. The
	// census is process-wide, so a test counts only its own bubble's changes.
	bubbled bool
}

// Before any real clock, after any bubble's.
var urnetworkTimerCensusBubbleEnd = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)

// Records every change the census hands it.
type urnetworkTimerCensusRecorder struct {
	mutex   sync.Mutex
	records []urnetworkTimerCensusRecord
}

func (r *urnetworkTimerCensusRecorder) record(site TimerCensusSite, horizon time.Duration, stop bool, count int) {
	bubbled := time.Now().Before(urnetworkTimerCensusBubbleEnd)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.records = append(r.records, urnetworkTimerCensusRecord{
		site:    site,
		horizon: horizon,
		stop:    stop,
		count:   count,
		bubbled: bubbled,
	})
}

// The changes at site that a goroutine in a bubble made, in order.
func (r *urnetworkTimerCensusRecorder) bubbledAt(site TimerCensusSite) []urnetworkTimerCensusRecord {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var records []urnetworkTimerCensusRecord
	for _, record := range r.records {
		if record.bubbled && record.site == site {
			records = append(records, record)
		}
	}
	return records
}

// Installs a census that records into a new recorder for the rest of the
// test.
func recordUrnetworkTimerCensus(t *testing.T, samplePeriod int) *urnetworkTimerCensusRecorder {
	t.Helper()
	recorder := &urnetworkTimerCensusRecorder{}
	SetTimerCensus(&TimerCensus{
		SamplePeriod: samplePeriod,
		Record:       recorder.record,
	})
	t.Cleanup(func() {
		SetTimerCensus(nil)
	})
	return recorder
}

// A Reset at site, d from now, recorded with a period of 1.
func urnetworkTimerCensusReset(site TimerCensusSite, d time.Duration) urnetworkTimerCensusRecord {
	return urnetworkTimerCensusRecord{site: site, horizon: d, count: 1, bubbled: true}
}

// A stop of a pending timer at site, recorded with a period of 1.
func urnetworkTimerCensusStop(site TimerCensusSite) urnetworkTimerCensusRecord {
	return urnetworkTimerCensusRecord{site: site, stop: true, count: 1, bubbled: true}
}

// A Read that waits re-arms its deadline timer at each wake, recorded with the
// time then left to the deadline, and its timer, pending when data ends the
// Read, is recorded as stopped once.
func TestUrnetworkTimerCensusCountsAStreamReadWait(t *testing.T) {
	testUrnetworkTimerCensusCountsAStreamReadWait(t, func(str *ReceiveStream) error {
		_, err := str.Read(make([]byte, 2))
		return err
	})
}

// The same for Peek, which waits as Read does.
func TestUrnetworkTimerCensusCountsAStreamPeekWait(t *testing.T) {
	testUrnetworkTimerCensusCountsAStreamReadWait(t, func(str *ReceiveStream) error {
		_, err := str.Peek(make([]byte, 2))
		return err
	})
}

func testUrnetworkTimerCensusCountsAStreamReadWait(t *testing.T, read func(*ReceiveStream) error) {
	synctest.Test(t, func(t *testing.T) {
		recorder := recordUrnetworkTimerCensus(t, 1)
		mockCtrl := gomock.NewController(t)
		str := newReceiveStream(42, NewMockStreamSender(mockCtrl), newTestStreamFlowController(42))

		deadline := time.Now().Add(time.Minute)
		require.NoError(t, str.SetReadDeadline(deadline))
		errChan := make(chan error, 1)
		go func() {
			errChan <- read(str)
		}()
		// the wait arms its timer, takes the wake SetReadDeadline left and
		// re-arms for the whole minute
		synctest.Wait()
		time.Sleep(13 * time.Second)
		// a wake with no data: the wait re-arms for the time left
		require.NoError(t, str.SetReadDeadline(deadline))
		synctest.Wait()
		require.NoError(t, str.handleStreamFrame(&wire.StreamFrame{Data: []byte{0xde, 0xad}}, monotime.Now()))
		require.NoError(t, <-errChan)

		require.Equal(t,
			[]urnetworkTimerCensusRecord{
				urnetworkTimerCensusReset(TimerCensusSiteStreamReadTimer, time.Minute),
				urnetworkTimerCensusReset(TimerCensusSiteStreamReadTimer, time.Minute-13*time.Second),
				urnetworkTimerCensusStop(TimerCensusSiteStreamReadTimer),
			},
			recorder.bubbledAt(TimerCensusSiteStreamReadTimer),
		)
	})
}

// A Read that its deadline ends received its timer's fire, so that timer is
// not recorded as stopped.
func TestUrnetworkTimerCensusDoesNotCountAFiredStreamReadTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recorder := recordUrnetworkTimerCensus(t, 1)
		str := newReceiveStream(42, nil, newTestStreamFlowController(42))

		require.NoError(t, str.SetReadDeadline(time.Now().Add(time.Minute)))
		_, err := str.Read(make([]byte, 2))
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)

		require.Equal(t,
			[]urnetworkTimerCensusRecord{
				urnetworkTimerCensusReset(TimerCensusSiteStreamReadTimer, time.Minute),
			},
			recorder.bubbledAt(TimerCensusSiteStreamReadTimer),
		)
	})
}

// A Write that waits for the peer re-arms its deadline timer at each wake that
// leaves it waiting, recorded with the time then left to the deadline; that
// timer, pending as the Write returns, is recorded as stopped once.
func TestUrnetworkTimerCensusCountsAStreamWriteWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recorder := recordUrnetworkTimerCensus(t, 1)
		mockCtrl := gomock.NewController(t)
		mockSender := NewMockStreamSender(mockCtrl)
		str := newSendStream(context.Background(), 42, mockSender, newTestStreamFlowControllerWithSendWindow(42, protocol.MaxByteCount), false)
		mockSender.EXPECT().onHasStreamData(gomock.Any(), str)

		deadline := time.Now().Add(time.Minute)
		require.NoError(t, str.SetWriteDeadline(deadline))
		errChan := make(chan error, 1)
		go func() {
			// larger than a frame it can buffer, so the Write waits
			_, err := str.Write(make([]byte, 2000))
			errChan <- err
		}()
		// the wait arms its timer, takes the wake SetWriteDeadline left and
		// re-arms for the whole minute
		synctest.Wait()
		time.Sleep(11 * time.Second)
		// a wake that takes no data: the wait re-arms for the time left
		require.NoError(t, str.SetWriteDeadline(deadline))
		synctest.Wait()
		// a packet's worth goes out, and the Write buffers the rest and returns
		frame, _, _ := str.popStreamFrame(1000, protocol.Version1)
		require.NotNil(t, frame.Frame)
		require.NoError(t, <-errChan)

		require.Equal(t,
			[]urnetworkTimerCensusRecord{
				urnetworkTimerCensusReset(TimerCensusSiteStreamWriteTimer, time.Minute),
				urnetworkTimerCensusReset(TimerCensusSiteStreamWriteTimer, time.Minute-11*time.Second),
				urnetworkTimerCensusStop(TimerCensusSiteStreamWriteTimer),
			},
			recorder.bubbledAt(TimerCensusSiteStreamWriteTimer),
		)
	})
}

// A Write that its deadline ends received its timer's fire, so that timer is
// not recorded as stopped.
func TestUrnetworkTimerCensusDoesNotCountAFiredStreamWriteTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recorder := recordUrnetworkTimerCensus(t, 1)
		mockCtrl := gomock.NewController(t)
		mockSender := NewMockStreamSender(mockCtrl)
		str := newSendStream(context.Background(), 42, mockSender, newTestStreamFlowControllerWithSendWindow(42, protocol.MaxByteCount), false)
		mockSender.EXPECT().onHasStreamData(gomock.Any(), str)

		require.NoError(t, str.SetWriteDeadline(time.Now().Add(time.Minute)))
		_, err := str.Write(make([]byte, 2000))
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)

		require.Equal(t,
			[]urnetworkTimerCensusRecord{
				urnetworkTimerCensusReset(TimerCensusSiteStreamWriteTimer, time.Minute),
			},
			recorder.bubbledAt(TimerCensusSiteStreamWriteTimer),
		)
	})
}

// resetTimer records a stop when it replaces a pending timer, and nothing when
// it keeps the timer for an unchanged deadline or replaces one that fired. It
// never moves a timer, so it records no Reset.
func TestUrnetworkTimerCensusCountsTheConnTimerStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recorder := recordUrnetworkTimerCensus(t, 1)
		c := &Conn{}
		c.timerDeadline = monotime.Now().Add(time.Second)
		c.timer = time.NewTimer(monotime.Until(c.timerDeadline))
		c.timerPending = true

		// the same deadline keeps the pending timer
		c.resetTimer(c.timerDeadline)
		require.Empty(t, recorder.bubbledAt(TimerCensusSiteConnTimer))

		// another deadline stops the pending timer
		c.resetTimer(monotime.Now().Add(2 * time.Second))
		require.Equal(t,
			[]urnetworkTimerCensusRecord{urnetworkTimerCensusStop(TimerCensusSiteConnTimer)},
			recorder.bubbledAt(TimerCensusSiteConnTimer),
		)

		// a timer that fired is replaced without a stop
		<-c.timer.C
		c.timerPending = false
		c.resetTimer(c.timerDeadline)
		require.Equal(t,
			[]urnetworkTimerCensusRecord{urnetworkTimerCensusStop(TimerCensusSiteConnTimer)},
			recorder.bubbledAt(TimerCensusSiteConnTimer),
		)
		c.timer.Stop()
	})
}

// A sample period that is not a positive power of two, or a census without a
// Record, is refused.
func TestUrnetworkTimerCensusRefusesAnInvalidCensus(t *testing.T) {
	t.Cleanup(func() {
		SetTimerCensus(nil)
	})
	record := func(TimerCensusSite, time.Duration, bool, int) {}
	// past 32 bits where an int has 64, and zero, refused too, where it has 32
	pastUint32 := uint64(math.MaxUint32) + 1
	for _, period := range []int{0, -1, -64, 3, 6, 100, int(pastUint32)} {
		require.Panics(t, func() {
			SetTimerCensus(&TimerCensus{SamplePeriod: period, Record: record})
		}, "sample period %d", period)
	}
	require.Panics(t, func() {
		SetTimerCensus(&TimerCensus{SamplePeriod: 64})
	})
	for _, period := range []int{1, 2, 64, 1 << 20} {
		require.NotPanics(t, func() {
			SetTimerCensus(&TimerCensus{SamplePeriod: period, Record: record})
		}, "sample period %d", period)
	}
}

// A removed census records nothing.
func TestUrnetworkTimerCensusRemovedRecordsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recorder := recordUrnetworkTimerCensus(t, 1)
		SetTimerCensus(nil)
		for site := range TimerCensusSiteCount {
			TimerCensusReset(site, time.Second)
			TimerCensusStop(site)
			require.Empty(t, recorder.bubbledAt(site))
		}
	})
}

// A census takes about one change in its sample period, and records each as
// that many.
func TestUrnetworkTimerCensusSamplesOneChangeInAPeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const samplePeriod = 8
		const changeCount = 8192
		const horizon = 17*time.Second + 3*time.Millisecond
		recorder := recordUrnetworkTimerCensus(t, samplePeriod)
		for range changeCount {
			TimerCensusReset(TimerCensusSiteStreamReadTimer, horizon)
			TimerCensusStop(TimerCensusSiteStreamWriteTimer)
		}

		resets := recorder.bubbledAt(TimerCensusSiteStreamReadTimer)
		stops := recorder.bubbledAt(TimerCensusSiteStreamWriteTimer)
		// a binomial count of mean 1024 and deviation 30 stays within 512
		for _, records := range [][]urnetworkTimerCensusRecord{resets, stops} {
			require.InDelta(t, changeCount/samplePeriod, len(records), changeCount/samplePeriod/2)
		}
		for _, record := range resets {
			require.Equal(t, urnetworkTimerCensusRecord{site: TimerCensusSiteStreamReadTimer, horizon: horizon, count: samplePeriod, bubbled: true}, record)
		}
		for _, record := range stops {
			require.Equal(t, urnetworkTimerCensusRecord{site: TimerCensusSiteStreamWriteTimer, stop: true, count: samplePeriod, bubbled: true}, record)
		}
	})
}
