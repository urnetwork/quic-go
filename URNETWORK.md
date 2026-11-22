# URnetwork's quic-go

This repository is URnetwork's fork of quic-go, the module
`github.com/urnetwork/quic-go`. It is upstream `github.com/quic-go/quic-go
v0.61.0` (module zip `h1:ui88A53s8MSVYLC56en0KQ17HARk+9986Dn0SBfKNvA=`)
with:

- these narrowly scoped changes, each described below:
  - the run loop timer, in `connection.go`;
  - one GOAWAY per connection in a graceful http3 shutdown, in
    `http3/server.go`;
  - the header phase of an http3 request stream, in `http3/server.go` with
    `http3/server_conn.go`;
  - one transport over a group of sockets, the added
    `urnetwork_conn_group.go`;
  - per-connection queue limits, in `interface.go`, `config.go`,
    `datagram_queue.go` and `connection.go`, with `config_test.go`;
  - a trace of the handshake only, the added `urnetwork_handshake_trace.go`
    and `urnetwork_stop_recording.go` in `internal/ackhandler`,
    `internal/congestion` and `internal/handshake`, with one call in
    `connection.go`;
- its own module path. Every internal import names
  `github.com/urnetwork/quic-go`, as do the mockgen directives, the lint
  config, the fuzzing and interop scripts and the qlog version lookup in
  `qlogwriter/trace.go`. A module that imports this one builds it whatever
  replace lines its consumers have, which a fork under upstream's path
  cannot do: Go ignores a dependency's replacements. Upstream
  `github.com/quic-go/quic-go` can sit beside it in one build, as a separate
  package, for libraries that import it;
- `go 1.27.4` in `go.mod`, the release toolchain of every URnetwork module
  (upstream declares `go 1.25.0`);
- upstream's `.github/` and its two nested CI modules,
  `integrationtests/fips` and `integrationtests/gomodvendor`, removed. Only
  upstream's workflows ran them, and the module zip never held them.

The tests of the changes are the added `urnetwork_conn_timer_test.go`,
`urnetwork_conn_group_test.go`, `urnetwork_conn_group_oob_test.go`,
`urnetwork_conn_group_linux_test.go`, `urnetwork_queue_limits_test.go`,
`urnetwork_handshake_trace_test.go`,
`http3/urnetwork_graceful_goaway_test.go` and
`http3/urnetwork_read_header_timeout_test.go`; `go test -run
'^TestUrnetwork' . ./http3/` runs them, and `go test -run '^TestUrnetwork'
./internal/...` the trace's in the internal packages. The upstream MIT license and source
headers are preserved. Upstream's `README.md`, `SECURITY.md` and
`FUZZING.md` are unchanged and describe upstream, and links to upstream
issues stay upstream's.

The changes were made in the server's `third_party/quic-go` (2026-11-04 to
2026-11-19) and in connect's copy of it (the queue limits, 2026-11-20); each
is one commit here, with the original date, and its tree is the copy's.

## Using it

A URnetwork main module requires the fork at `v0.0.0` and replaces it with
the sibling checkout, as it does glog:

```
require github.com/urnetwork/quic-go v0.0.0

replace github.com/urnetwork/quic-go => ../quic-go
```

and imports `github.com/urnetwork/quic-go`, `github.com/urnetwork/quic-go/http3`
and so on. Every main module that builds a module importing the fork needs
the replace and the `../quic-go` checkout: Go ignores a dependency's
replacements.

## Releases

The release (the build repository's `all/run.sh`) handles this repository as
it handles glog. It pulls `main`, branches `v<version>`, renames the module in
place to `github.com/urnetwork/quic-go/v2026` and rewrites the internal
imports to match, then commits and tags `v<version>` (for example
`v2026.11.21-1104000000`) and pushes the branch and tag before it edits
connect. Each published module that requires the fork then drops its replace
and requires `github.com/urnetwork/quic-go/v2026` at that version, so a
consumer of a published connect or SDK resolves the fork from the tag with no
replace of its own. `main` itself is never tagged.

## Updating from upstream

Bring a newer upstream tag in from the `upstream` remote onto `main`, keep
the changes below and the module path (new upstream files need their imports
renamed), and run the tests here, `go test -short ./...` among them.

## The run loop timer

A connection's run loop calls `maybeResetTimer` on each iteration, and
upstream re-arms the loop's timer with `c.timer.Reset(monotime.Until(deadline))`
whether the deadline changed or not. The loop waits on the timer's channel in
a select, so the channel timer is in the runtime's timer heap while the loop
waits, and stays there between waits until it fires or is stopped. A `Reset`
of a timer in the heap to another time makes its P scan every timer in the
heap (`runtime.(*timers).adjust`). In a connect process, with about 32,000
timers per P, these resets were 37–54% of the re-arms that cause those scans
(CPUOPT4 Part A §3.4).

The change, all in `connection.go`:

- `resetTimer(deadline)` replaces the three `Reset` calls in
  `maybeResetTimer`. A timer still pending for the same deadline is kept: it
  fires at that deadline, which is when the `Reset` would have armed it for,
  and if the deadline has passed its value is received at once, as the `Reset`
  made it fire at once. Any other deadline, or the same deadline after the
  timer fired, stops the timer and arms a new one with `time.NewTimer` for that
  deadline. The stopped timer leaves the heap at its time; the new one is an
  insert.
- The `Conn` keeps `timerDeadline` and `timerPending`, set where the timer is
  armed; the run loop clears `timerPending` when it receives from the timer's
  channel.

No deadline computation, wake-up condition or timer instant changes.

### Tests

`urnetwork_conn_timer_test.go`, run with `go test -run '^TestUrnetwork' .` at
the repository root:

- under virtual time, a loop like the connection's, with deadlines that stay,
  move earlier or later, or are already due, and events that wake the loop
  first, fires at the same instants with `resetTimer` as with upstream's
  `Reset`, over 100 seeds of 1,000 iterations;
- an unchanged pending deadline keeps its timer; a changed one, or one whose
  timer fired, gets a new timer and the last is stopped.

The upstream suite passes: `go test -short .` here ran 742 tests with this
change.

## One GOAWAY per connection in a graceful http3 shutdown

`Server.handleConn` accepts a connection's request streams on the server's
grace context until a graceful shutdown (`Shutdown`) cancels it. The accept
then fails, and the loop's graceful branch starts a goroutine that writes a
GOAWAY on the control stream and switches the accept to the server's close
context, so later streams are refused while the requests in flight finish.

quic-go closes a closing connection's streams before it cancels the
connection's context. Between the two, an accept fails at once while the
connection's context and the close context are both live, and upstream's loop
took the graceful branch again on every pass: another GOAWAY writer each time,
for as long as the window stayed open. Every connection that closes during a
graceful shutdown passes through that window: the client closing after the
GOAWAY, the idle timeout, or a transport destroyed under it. The window stays
open while the transport's close holds its mutex over its destroy fan-out, and
while every core is busy, which the spinning loops make worse. URnetwork's alt
had an exit burst of at least 700k goroutines this way (10-24 to 10-26), and
the connect front's drain (server LBALT.md D12) shuts each api connection down
gracefully while its transport is live.

The change, in `http3/server.go`: once the graceful branch has run, so the
GOAWAY was sent and the accept waits on the close context, an accept failure
with the close context live can only be the connection closing, and the loop
ends as it does once the connection's context is canceled, with the same error
classification. A second GOAWAY is never created. Nothing changes before a
graceful shutdown, or for a close of the server.

### Tests

`http3/urnetwork_graceful_goaway_test.go`, run with
`go test -run '^TestUrnetwork' ./http3/` at the repository root. A server recorder
holds the connection inside its close window (quic-go records the close between
closing the streams and canceling the context) and counts the GOAWAYs the
server creates. After a graceful shutdown's GOAWAY, the client closes its
connection in one test and the server's transport loses its socket in the
other. `ServeQUICConn` must return while the window is held, with one GOAWAY;
upstream's loop creates a second one at once and never returns while held.

The upstream http3 suite passes with this change: `go test -short ./http3/...`
here, under `GOFLAGS=-trimpath`, skipping the six tests that read source paths
at run time (`TestErrorCodes`, `TestServerAltSvcFromUnixSocket`,
`TestServerClosing`, `TestServerConcurrentServeAndClose`,
`TestServerImmediateGracefulShutdown`, `TestServerListenAndServeErrors`).

## The header phase of an http3 request stream

`RawServerConn.handleRequestStream` stops the connection's idle timer when a
request stream starts, parses the stream's first frame, and reads the
HEADERS frame's field section into a buffer of the length the frame
announces, up to `MaxHeaderBytes`. Upstream did both with no deadline. A
stream that announced a length and sent nothing more held its goroutine and
that buffer, and with the idle timer stopped its connection too, for as long
as the peer acked the connection's PINGs, which the connect front's public
QUIC config sends, so the QUIC idle timeout never fired either. An
unauthenticated address could pin about 7 MiB and 100 goroutines per api
connection to the front this way (server LBALT final review).

The change, in `http3/server.go` and `http3/server_conn.go`:

- `Server.ReadHeaderTimeout`, passed to each `RawServerConn`. When positive,
  a request stream's read deadline is set to the stream's start plus the
  timeout before its first frame is parsed, and cleared once the request is
  parsed, before the handler runs, which bounds its own reads of the body. A
  stream past it takes the existing `H3_REQUEST_INCOMPLETE` path, its
  goroutine ends, and with no stream left the idle timer re-arms. The bound
  also covers unknown frames skipped before the HEADERS frame.
- `readHeaderBlock` reads the field section through an `io.LimitReader` of
  the announced length with `io.ReadAll`, so the buffer grows with the bytes
  that arrive, at most about twice what was received, rather than taking the
  announced length up front. A stream that ends first is
  `io.ErrUnexpectedEOF`, on the same error path as before.

With `ReadHeaderTimeout` unset, only the read of the field section changes.

### Tests

`http3/urnetwork_read_header_timeout_test.go`, run with
`go test -run '^TestUrnetwork' ./http3/` at the repository root:

- a stream that sends only a HEADERS frame's type and announced length is
  reset with `H3_REQUEST_INCOMPLETE` once `ReadHeaderTimeout` passes, and its
  handler never runs; upstream never resets it;
- a handler that reads its body only after waiting out `ReadHeaderTimeout`
  still reads it whole, so the deadline ends with the parsed request;
- a field section announcing 64 KiB of which ten bytes arrive never asks for a
  read larger than 4 KiB, where upstream asked for 64 KiB at once; a whole
  section is read exactly, and a stream that ends first is an error.

The upstream http3 suite passes with this change: `go test -count=1
./http3/...` here, with no `-trimpath`, since some of its tests read source
paths at run time.

## One transport over a group of sockets

`ConnGroup`, in the added `urnetwork_conn_group.go`, is the packet conn of one
`Transport` over several UDP sockets bound to one local address and port, such
as the sockets of one SO_REUSEPORT group. No upstream file changes.

The kernel spreads a reuseport group's flows over its sockets by a hash of
each flow's 4-tuple, and moves most of them whenever a socket leaves or joins
the group; a NAT rebinding moves one. With a `Transport` per socket, a moved
flow reaches a transport that doesn't hold its connection IDs, and the client
reconnects only after its idle timeout. The connect front (server LBALT.md
D22 (b), LBALT-IMPL.md §8) serves each group of its public sockets on the
QUIC port through one `ConnGroup` and one `Transport`, so a moved flow keeps
its connection.

A plain `net.PacketConn` fan-in would lose quic-go's per-socket paths, since
`Transport` wraps any conn that isn't OOB-capable as a basic conn, and its oob
conn reads one socket through that socket's `SyscallConn`. `ConnGroup` keeps
them:

- `ConnGroup.Add` wraps each socket as `Transport.init` wraps the conn it is
  given (`wrapConn`): the 7 MiB buffers, the DF bit, ECN and packet info
  reception, batched reads and the GSO probe, so a `*net.UDPConn` is read and
  written through its own oob conn. Each socket is read on its own goroutine
  into one queue of 256 packets; `ConnGroup` implements `rawConn`, so
  `Transport` uses it as it is, and its one listen loop reads the queue.
- A write goes out through one live socket, picked by a seeded hash of the
  destination address and port, so one flow leaves through one socket while
  the group's sockets stay the same. The GSO segment size, the ECN marking and
  the packet info pass to that socket's own write unchanged, and so does every
  error but one: a socket closed under the write (`net.ErrClosed`) is skipped
  for the next. With no socket left the packet is dropped, as the network
  drops one, since a write error ends the connection that wrote; once the
  group is closed, writes fail with `net.ErrClosed`.
- A socket whose read fails leaves the group at once, and its
  `ConnGroupMember` reports the error (`Done`, `Err`); temporary errors are
  retried, as `Transport.listen` retries them. `Add` brings in a replacement
  while the transport serves. `ConnGroupMember.Close` removes and closes one
  socket, and `ConnGroup.Close` every socket.
- The group's capabilities (DF, GSO, ECN) are its first socket's, and a socket
  with others is refused: a connection reads them when it starts, and a GSO
  batch must never reach a socket without GSO.
- The group never closes on its own. `Transport.Close` stops its listen loop
  with a read deadline in the past, as on a socket, and the group keeps its
  sockets until it is closed itself.

### Tests

`urnetwork_conn_group_test.go`, `urnetwork_conn_group_oob_test.go` (darwin,
linux, freebsd) and `urnetwork_conn_group_linux_test.go`, run with
`go test -run '^TestUrnetwork' .` at the repository root. A relay stands in for the
kernel's choice of socket, and for a NAT, so a flow moves between sockets
bound to distinct ports when a test says, whatever the platform's reuseport
delivery:

- a connection keeps echoing when its flow moves to another socket of the
  group, and again after a rebinding changes its source port as well;
- a socket whose reads fail reports the error and leaves the write set, and
  the connection it carried goes on through a sibling;
- one destination's writes leave through one socket, then through another
  once that socket is lost, are dropped without an error with no socket left,
  and fail once the group is closed; a socket closed under a write is skipped;
- a write passes its GSO size, ECN marking and packet info to its socket, and
  a group reports its first socket's capabilities and refuses a socket with
  others;
- every socket is read through its oob conn, with the ECN bits of each
  datagram, and the group's capabilities are a lone socket's;
- `Transport.Close` over the group leaves its sockets read on, and closing the
  group ends them, and its reads, with `net.ErrClosed`;
- on Linux, a GSO batch leaves as segments of the asked size where the kernel
  supports GSO (not run on darwin).

Each fails when its behavior is taken out of `urnetwork_conn_group.go`. The
upstream suite passes with this addition: `go test -short .` here ran 747
tests.

## Per-connection queue limits

A connection holds three queues whose lengths upstream fixes:

- the packets it has received and its run loop has not processed yet, at
  most `protocol.MaxConnUnprocessedPackets`, 256, each in a 1452-byte
  packet buffer;
- the DATAGRAM frames queued for sending, 32, each a copy of the payload;
- the received DATAGRAM frames that `ReceiveDatagram` has not read, 128,
  each a copy of the payload.

At full-size packets that is about 384 KiB of unprocessed packets and
240 KiB of DATAGRAMs that one connection may hold, whatever its flow-control
windows. A phone runs a QUIC connection per peer, six to ten of them in one
memory budget that charges each connection what it can hold (connect
`transport_h3_memory.go`, server LBALTOPT2 R20), so the apps set smaller
queues.

The change:

- `Config.MaxUnprocessedPackets`, `Config.MaxDatagramSendQueueLen` and
  `Config.MaxDatagramReceiveQueueLen`, in `interface.go`. Zero or negative
  takes upstream's length, so a config that does not set them behaves as
  upstream does. `populateConfig` copies them and fills the defaults
  (`config.go`). An unprocessed-packet limit below 32 is raised to 32, one
  more than `protocol.Max0RTTQueueLen`, so a server hands a new connection
  every 0-RTT packet it buffered for it, as upstream requires of the
  constant.
- `connection.go`: `handlePacket` drops a packet beyond the connection's
  limit where it compared with the constant, and the connection builds its
  DATAGRAM queue with its configured lengths.
- `datagram_queue.go`: the queue keeps its two lengths, which
  `newDatagramQueueWithLimits` takes; `newDatagramQueue` keeps upstream's.
  The constants keep their upstream names as the defaults.

### Tests

`urnetwork_queue_limits_test.go`, run with `go test -run '^TestUrnetwork' .`
at the repository root:

- a config that leaves the limits unset, or sets them negative, gets 256, 32
  and 128; set limits are kept; an unprocessed-packet limit of 1 becomes 32;
- a connection's DATAGRAM queue takes its config's lengths, and upstream's
  without them;
- a send queue of three admits three frames and blocks the fourth until one
  leaves; a receive queue of five keeps five of seven frames;
- a connection with a limit of 40 keeps 40 unprocessed packets and drops the
  41st, as upstream drops the 257th.

`config_test.go` lists the three fields in `configWithNonZeroNonFunctionFields`,
which fails on a field it does not know, and checks their defaults in
`TestConfigDefaultValues`.

## A trace of the handshake only

quic-go builds each event of a traced connection, a qlog event with its
frames and, for a long header packet, the datagram's checksum, before it
hands the event to the connection's recorder, so a trace pays that whatever
it keeps. The connect front classifies its whodis handshakes that fail from
a trace of each whodis connection (server `connect/front_quic_trace.go`), and
it wants nothing of an established connection: there, the events cost about
four allocations a packet for the connection's whole life.

`HandshakeOnlyTrace`, in the added `urnetwork_handshake_trace.go`, is an
optional interface of the `qlogwriter.Trace` that `Config.Tracer` returns:
`HandshakeOnly() bool`. When the handshake completes, the connection's
`handleHandshakeComplete` (`connection.go`, one deferred call that runs
before `HandshakeComplete` closes) asks it, on the run loop. When it reports
true:

- the connection's sent packet handler, its congestion controller and its ECN
  tracker (`ackhandler.StopRecording`, which `congestion.StopRecording`
  serves), its crypto setup and 1-RTT keys (`handshake.StopRecording`) and
  its path MTU discoverer drop the recorder, each in an added
  `urnetwork_stop_recording.go` or the new file, so none of them builds an
  event from then on; a congestion controller built after a path migration
  takes the handler's recorder, which is then none;
- the connection drops its own recorder and closes it, under
  `receivedPacketMx`, since `handlePacket` reads and records through it on
  the transport's goroutine under that lock. `run` then finds no recorder to
  close, and the connection records no `ConnectionClosed`.

A trace that doesn't implement the interface, or reports false, records the
connection's whole life as before, and an untraced connection is untouched.
An http3 producer, which http3 adds only to a trace that supports its schema,
is http3's and keeps recording.

### Tests

`urnetwork_handshake_trace_test.go`, run with `go test -run '^TestUrnetwork'
.`, and `urnetwork_stop_recording_test.go` in `internal/ackhandler`,
`internal/congestion` and `internal/handshake`:

- over real connections on loopback, a handshake-only trace on a server and
  on a client records the handshake, is closed once when it completes,
  before `HandshakeComplete`, and gets no event for 100 KB of data, the
  path MTU discovery or the close;
- a trace without `HandshakeOnly`, and one reporting false, gets 1-RTT
  packets after the handshake, RTT samples and the close, an event for every
  packet the connection sent (`ConnectionStats().PacketsSent`), and is closed
  once, with the connection;
- `endHandshakeOnlyTrace` drops the recorder of every part of a connection,
  the path MTU discoverer's included, which loopback may never exercise;
  each part then records nothing, and a trace that doesn't opt in keeps
  every recorder;
- each `StopRecording` stops its part's events, a congestion controller
  built after a migration included, and leaves a part of another type.

Each fails when its stop is taken out. `BenchmarkUrnetworkHandshakeOnlyTrace`
streams 1000-byte writes to a server: a handshake-only trace allocates what
an untraced connection does (16 to 17 allocations and about 690 bytes a
write) and gets no event, where a trace of every event costs 20 to 21
allocations and about 1,060 bytes, with 1.3 to 1.4 events a write.
