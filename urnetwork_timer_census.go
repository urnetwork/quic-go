package quic

import (
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"sync/atomic"
	"time"
)

// urnetwork: a sampled count of the changes quic-go makes to Go runtime timers
// on its hot paths, by where it makes them and how far ahead each changed
// timer is due, for a process that measures what sets off the runtime's timer
// heap walks (server CPUOPT7.md §11). See URNETWORK.md, "A census of timer
// changes".
//
// A Reset of a timer that is pending, or still in its P's heap as a zombie,
// marks the timer modified, and once the earliest modified deadline on a P
// passes, that P's next timer check walks its whole heap. A stop, or a select
// that wakes on another case, leaves a zombie that a walk or the heap's head
// removes; a new timer is an insert. Neither sets off a walk.
//
// The census is process-wide because the runtime's timer heaps are. Without
// one installed, a counted change costs an atomic load. With one, a change is
// sampled through the calling thread's random source (math/rand/v2), with no
// lock and no allocation, and only a sampled change reaches Record.

// A TimerCensusSite is where quic-go changes a runtime timer on a hot path: a
// fixed set of TimerCensusSiteCount values.
type TimerCensusSite int

const (
	// TimerCensusSiteStreamReadTimer is the timer of a stream Read or Peek
	// that waits under a read deadline.
	TimerCensusSiteStreamReadTimer TimerCensusSite = iota
	// TimerCensusSiteStreamWriteTimer is the timer of a stream Write that
	// waits under a write deadline.
	TimerCensusSiteStreamWriteTimer
	// TimerCensusSiteConnTimer is a connection's run loop timer. resetTimer
	// stops a pending timer and arms a new one for a new deadline, and never
	// moves a pending timer, so it counts only stops.
	TimerCensusSiteConnTimer
	// TimerCensusSiteHTTP3IdleTimer is an http3 server connection's idle
	// timer, stopped when a request stream starts and reset when the
	// connection's last stream ends.
	TimerCensusSiteHTTP3IdleTimer

	TimerCensusSiteCount
)

// A TimerCensus is a process's census of quic-go's timer changes.
type TimerCensus struct {
	// One change in SamplePeriod is recorded, as SamplePeriod changes. A
	// positive power of two; 1 records every change.
	SamplePeriod int
	// Record records count changes at site: a timer set to fire horizon from
	// now (a nonpositive horizon is due at once), or, when stop, a pending
	// timer stopped, with horizon zero. It is called on the goroutine making
	// the change, possibly concurrently, so it must not block.
	Record func(site TimerCensusSite, horizon time.Duration, stop bool, count int)
}

// The installed census, as the counted changes read it.
type installedTimerCensus struct {
	// a sampled change has these bits of a random number clear
	mask   uint32
	count  int
	record func(site TimerCensusSite, horizon time.Duration, stop bool, count int)
}

var timerCensus atomic.Pointer[installedTimerCensus]

// SetTimerCensus installs census for the process, or removes the installed
// one when nil. It panics on a SamplePeriod that is not a positive power of
// two or a nil Record.
func SetTimerCensus(census *TimerCensus) {
	if census == nil {
		timerCensus.Store(nil)
		return
	}
	period := census.SamplePeriod
	if period <= 0 || period&(period-1) != 0 || math.MaxUint32 < uint64(period) {
		panic(fmt.Errorf("timer census: the sample period %d is not a positive power of two", period))
	}
	if census.Record == nil {
		panic(fmt.Errorf("timer census: no Record"))
	}
	timerCensus.Store(&installedTimerCensus{
		mask:   uint32(period - 1),
		count:  period,
		record: census.Record,
	})
}

// TimerCensusReset counts a Reset at site that arms its timer d from now. It
// is exported for the fork's http3 package.
func TimerCensusReset(site TimerCensusSite, d time.Duration) {
	census := timerCensus.Load()
	if census == nil {
		return
	}
	if mathrand.Uint32()&census.mask != 0 {
		return
	}
	census.record(site, d, false, census.count)
}

// TimerCensusStop counts a stop of a pending timer at site. It is exported for
// the fork's http3 package.
func TimerCensusStop(site TimerCensusSite) {
	census := timerCensus.Load()
	if census == nil {
		return
	}
	if mathrand.Uint32()&census.mask != 0 {
		return
	}
	census.record(site, 0, true, census.count)
}

// Re-arms timer d from now, as the Reset it stands in for does, and counts the
// Reset at site.
func resetCensusTimer(timer *time.Timer, d time.Duration, site TimerCensusSite) {
	timer.Reset(d)
	TimerCensusReset(site, d)
}

// Stops timer, as the deferred Stop it stands in for does, and counts the stop
// at site when the timer was pending.
func stopCensusTimer(timer *time.Timer, site TimerCensusSite) {
	if timer.Stop() {
		TimerCensusStop(site)
	}
}
