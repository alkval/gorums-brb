package bench

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relab/gorums/benchkit"
)

const procHeader = "  sl  local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"

func endpointHex(raw string, little bool) string {
	a := netip.MustParseAddrPort(raw)
	b := a.Addr().AsSlice()
	if little {
		for i := 0; i < len(b); i += 4 {
			b[i], b[i+3] = b[i+3], b[i]
			b[i+1], b[i+2] = b[i+2], b[i+1]
		}
	}
	return fmt.Sprintf("%s:%04X", strings.ToUpper(hex.EncodeToString(b)), a.Port())
}

func socketFixture(local, remote string, state int, inode uint64) string {
	return fmt.Sprintf("0: %s %s %02X 00000012:00000034 01:000000AB 00000002 1000 3 %d 1 00000000\n",
		endpointHex(local, nativeLittleEndian()), endpointHex(remote, nativeLittleEndian()), state, inode)
}

func TestProcEndpointByteOrder(t *testing.T) {
	for raw, want := range map[string]string{"010200C0:2328": "192.0.2.1:9000", "0000000000000000FFFF0000010200C0:2328": "192.0.2.1:9000", "B80D012000000000000000000A000000:2328": "[2001:db8::a]:9000"} {
		a, err := procEndpoint(raw, true)
		if err != nil || a.String() != want {
			t.Fatal(raw, a, err)
		}
	}
	for _, raw := range []string{"192.0.2.1:9000", "[2001:db8::a]:9000", "[::ffff:192.0.2.1]:45001"} {
		for _, little := range []bool{false, true} {
			a, err := procEndpoint(endpointHex(raw, little), little)
			want := netip.MustParseAddrPort(raw)
			want = netip.AddrPortFrom(want.Addr().Unmap(), want.Port())
			if err != nil || a != want {
				t.Fatalf("%s little=%t got %v %v", raw, little, a, err)
			}
		}
	}
	for _, raw := range []string{"", "AA:GG", "0:1234", "00000000:10000", "XXXX:00"} {
		if _, err := procEndpoint(raw, true); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}

func TestParseNetSockets(t *testing.T) {
	data := procHeader + socketFixture("192.0.2.1:45001", "192.0.2.2:9000", 1, 42) +
		socketFixture("192.0.2.1:9000", "0.0.0.0:0", 10, 9) + socketFixture("192.0.2.1:45002", "192.0.2.2:9000", 6, 0)
	s, rows, err := parseSockets([]byte(data), nativeLittleEndian())
	if err != nil || rows != 3 || len(s) != 2 {
		t.Fatalf("%+v %d %v", s, rows, err)
	}
	if s[0].tx != 18 || s[0].rx != 52 || s[0].timer != 1 || s[0].ticks != 171 || s[0].rto != 2 || s[0].probes != 3 || s[0].inode != 42 {
		t.Fatalf("wrong fields: %+v", s[0])
	}
	for _, bad := range []string{"", procHeader + "short\n", strings.Replace(data, "00000012:00000034", "bad", 1), strings.Replace(data, "00000002 1000", "xyz 1000", 1), strings.Replace(data, "1000 3 42", "1000 nope 42", 1), strings.Replace(data, "1000 3 42", "1000 3 nope", 1)} {
		if _, _, err := parseSockets([]byte(bad), nativeLittleEndian()); err == nil {
			t.Fatal("accepted malformed rows")
		}
	}
	if _, _, err := parseSockets(bytes.Repeat([]byte{'x'}, netFileBytes+1), true); parseStatus(err) != "byte_limit" {
		t.Fatal(err)
	}
	if _, _, err := parseSockets([]byte(procHeader+strings.Repeat(socketFixture("192.0.2.1:1", "192.0.2.2:9000", 6, 0), netFileRows+1)), nativeLittleEndian()); parseStatus(err) != "entry_limit" {
		t.Fatal(err)
	}
}

func TestNetMembershipAndPairing(t *testing.T) {
	peers := []string{"192.0.2.1:9000", "192.0.2.2:9000"}
	a, _ := newNetMembership(1, peers)
	b, _ := newNetMembership(2, peers)
	s := procSocket{local: netip.MustParseAddrPort("192.0.2.1:45001"), remote: b.self, state: 1, tx: 12, rx: 34}
	x, ok := a.match(s)
	if !ok || x.Peer != 2 || x.Role != "dialed" {
		t.Fatal(x)
	}
	s.local, s.remote = s.remote, s.local
	y, ok := b.match(s)
	if !ok || y.Role != "accepted" || x.socketKey != y.socketKey {
		t.Fatalf("reverse pair: %+v %+v", x, y)
	}
	s = procSocket{local: a.self, remote: netip.MustParseAddrPort("192.0.2.2:45002"), state: 6}
	z, ok := a.match(s)
	if !ok || z.Dialer != 2 || z.Acceptor != 1 || z.TX != nil || z.FieldStatus != "state_only" {
		t.Fatal(z)
	}
	for _, pair := range [][2]string{{"192.0.2.1:3000", "192.0.2.2:4000"}, {"192.0.2.1:9000", "192.0.2.9:45000"}, {"192.0.2.9:45000", "192.0.2.2:9000"}} {
		if _, ok := a.match(procSocket{local: netip.MustParseAddrPort(pair[0]), remote: netip.MustParseAddrPort(pair[1])}); ok {
			t.Fatal("included unrelated socket")
		}
	}
	shared, _ := newNetMembership(1, []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"})
	r, ok := shared.match(procSocket{local: shared.self, remote: netip.MustParseAddrPort("127.0.0.1:45000"), state: 1})
	if !ok || !r.Ambiguous || r.Peer != 0 {
		t.Fatal(r)
	}
	for _, peers := range [][]string{{"host:9000"}, {"192.0.2.1:9000", "192.0.2.1:9000"}} {
		if _, err := newNetMembership(1, peers); err == nil {
			t.Fatal("invalid membership accepted")
		}
	}
}

func TestNetCountersAndDelta(t *testing.T) {
	good := []byte("Tcp: MaxConn OutSegs RetransSegs\nTcp: -1 300 20\n")
	v, err := parseCounters(good, "Tcp", []string{"RetransSegs", "OutSegs"})
	if err != nil || v["OutSegs"] != 300 || v["RetransSegs"] != 20 {
		t.Fatal(v, err)
	}
	for _, bad := range []string{"Tcp: OutSegs\n", "Tcp: OutSegs OutSegs\nTcp: 1 2\n", "Tcp: OutSegs RetransSegs\nTcp: 1 nope\n", "Tcp: OutSegs RetransSegs\nTcp: 1\n", string(good) + string(good)} {
		if _, err := parseCounters([]byte(bad), "Tcp", []string{"RetransSegs", "OutSegs"}); err == nil {
			t.Fatal("bad counters accepted")
		}
	}
	for _, tc := range []struct {
		now, prev *uint64
		status    string
		delta     *uint64
	}{{nil, nil, "unavailable", nil}, {new(uint64(2)), nil, "initial", nil}, {new(uint64(2)), new(uint64(5)), "reset", nil}, {new(uint64(7)), new(uint64(5)), "ok", new(uint64(2))}} {
		r := counterDelta("x", "ok", tc.now, tc.prev)
		if r.DeltaStatus != tc.status || (r.Delta == nil) != (tc.delta == nil) || r.Delta != nil && *r.Delta != *tc.delta {
			t.Fatal(r)
		}
	}
}

type recordSink struct {
	mu      sync.Mutex
	records []netRecord
}

func (s *recordSink) write(line []byte) error {
	_, raw, ok := bytes.Cut(line, []byte(" diag net "))
	if !ok {
		return errors.New("prefix")
	}
	var r netRecord
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return err
	}
	if bytes.Contains(line, []byte("192.0.2")) || bytes.Contains(line, []byte("inode")) || bytes.Contains(line, []byte("uid")) {
		return errors.New("private field")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, r)
	return nil
}
func (s *recordSink) snapshot() []netRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]netRecord{}, s.records...)
}

func fixtureRead(_ context.Context, name string) ([]byte, error) {
	switch name {
	case "tcp":
		return []byte(procHeader + socketFixture("192.0.2.1:45001", "192.0.2.2:9000", 1, 42)), nil
	case "tcp6":
		return []byte(procHeader), nil
	case "snmp":
		return []byte("Tcp: RetransSegs OutSegs\nTcp: 10 100\n"), nil
	case "netstat":
		return []byte("TcpExt: TCPTimeouts\nTcpExt: 3\n"), nil
	default:
		return nil, errors.New("unknown fixture")
	}
}
func testNetCollector(t *testing.T, interval time.Duration) (*netCollector, *recordSink) {
	t.Helper()
	sink := &recordSink{}
	c, err := makeNetCollector(1, []string{"192.0.2.1:9000", "192.0.2.2:9000"}, interval, fixtureRead, sink.write, func() {})
	if err != nil {
		t.Fatal(err)
	}
	return c, sink
}

func TestNetRoundsFailuresAndLifecycle(t *testing.T) {
	c, sink := testNetCollector(t, time.Second)
	start := time.Now()
	if !c.collect(start, 0, "initial", nil) {
		t.Fatal(c.err)
	}
	if !c.collect(start, 1, "periodic", nil) {
		t.Fatal(c.err)
	}
	c.read = func(ctx context.Context, name string) ([]byte, error) {
		if name == "tcp" {
			return []byte(procHeader + socketFixture("192.0.2.1:45001", "192.0.2.2:9000", 1, 99)), nil
		}
		if name == "snmp" {
			return nil, errors.New("sensitive path")
		}
		return fixtureRead(ctx, name)
	}
	c.collect(start, 2, "periodic", nil)
	r := sink.snapshot()
	s := r[len(r)-2].Socket
	if s.Generation != 2 || !s.Reconnect {
		t.Fatal(s)
	}
	footer := r[len(r)-1].Summary
	if footer.Status != "partial" || footer.Counters[0].Value != nil || footer.Counters[0].DeltaStatus != "unavailable" {
		t.Fatal(footer)
	}
	c.read = fixtureRead
	c.collect(start, 3, "final", nil)
	r = sink.snapshot()
	if r[len(r)-1].Summary.Counters[0].DeltaStatus != "initial" {
		t.Fatal("missing baseline used for delta")
	}
	for _, mode := range []string{"malformed", "bytes", "entries", "duplicate", "new-port", "time-wait"} {
		t.Run(mode, func(t *testing.T) {
			c, sink := testNetCollector(t, time.Second)
			c.collect(start, 0, "initial", nil)
			c.read = func(ctx context.Context, name string) ([]byte, error) {
				if name != "tcp" {
					return fixtureRead(ctx, name)
				}
				switch mode {
				case "malformed":
					return []byte(procHeader + "bad\n"), nil
				case "bytes":
					return bytes.Repeat([]byte{'x'}, netFileBytes+1), nil
				case "entries":
					return []byte(procHeader + strings.Repeat(socketFixture("192.0.2.1:45001", "192.0.2.2:9000", 6, 0), netFileRows+1)), nil
				case "duplicate":
					row := socketFixture("192.0.2.1:45001", "192.0.2.2:9000", 1, 42)
					return []byte(procHeader + row + row), nil
				case "new-port":
					return []byte(procHeader + socketFixture("192.0.2.1:45002", "192.0.2.2:9000", 1, 43)), nil
				default:
					return []byte(procHeader + socketFixture("192.0.2.1:45001", "192.0.2.2:9000", 6, 0)), nil
				}
			}
			c.collect(start, 1, "final", nil)
			r := sink.snapshot()
			last := r[len(r)-1].Summary
			if mode != "time-wait" && last.Status != "partial" {
				t.Fatal(last)
			}
			if mode == "time-wait" && r[len(r)-2].Socket.TX != nil {
				t.Fatal("TIME_WAIT queue interpreted")
			}
		})
	}
}

func TestNetCollectorLifecycleAndMissedRounds(t *testing.T) {
	c, sink := testNetCollector(t, 10*time.Millisecond)
	entered, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	c.read = func(ctx context.Context, name string) ([]byte, error) {
		if reads.Add(1) == 1 {
			close(entered)
			<-release
		}
		return fixtureRead(ctx, name)
	}
	c.begin(time.Now())
	<-entered
	time.Sleep(40 * time.Millisecond) // deliberately miss ticks in one bounded worker
	close(release)
	deadline := time.After(time.Second)
	for reads.Load() < 8 {
		select {
		case <-deadline:
			t.Fatal("no periodic round")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := c.stop(); err != nil {
		t.Fatal(err)
	}
	r := sink.snapshot()
	if r[0].Phase != "initial" || r[len(r)-1].Phase != "final" {
		t.Fatal("missing endpoints")
	}
	found := false
	for _, x := range r {
		if x.Missed != nil {
			found = true
			if x.Missed.Count != x.Missed.Last-x.Missed.First+1 {
				t.Fatal(x)
			}
		}
	}
	if !found {
		t.Fatal("missed rounds not recorded")
	}
	n := len(r)
	if err := c.stop(); err != nil || len(sink.snapshot()) != n {
		t.Fatal("stop not idempotent")
	}
}

func TestSlowNetCollectorCannotDelayGuardOrStop(t *testing.T) {
	c, sink := testNetCollector(t, time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	c.read = func(ctx context.Context, name string) ([]byte, error) {
		if name == "tcp" {
			close(entered)
			<-release
		}
		return fixtureRead(ctx, name)
	}
	c.joinTimeout = 20 * time.Millisecond
	s, ctx := testSampler(t, 5*time.Millisecond, resourceLimits{goroutines: 1}, func() resources { return resources{goroutines: 2} })
	s.net = c
	s.printf = func(string, ...any) {}
	s.begin(time.Now())
	<-entered
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("collector blocked guard")
	}
	start := time.Now()
	s.stop()
	if time.Since(start) > time.Second || s.stopReason() != "goroutines" {
		t.Fatalf("stop reason: %v", s.stopReason())
	}
	close(release)
	select {
	case <-c.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after read released")
	}
	r := sink.snapshot()
	if len(r) != 1 || r[0].Kind != "terminal" || r[0].Terminal.Status != "shutdown_timeout" {
		t.Fatal("want terminal only after abandoned read", r)
	}
}

func TestNetCollectorOutputFailureAndNeverBegun(t *testing.T) {
	c, _ := testNetCollector(t, time.Second)
	var closed atomic.Bool
	c.close = func() { closed.Store(true) }
	if err := c.stop(); err != nil || !closed.Load() || c.shutdownStatus != "not_started" {
		t.Fatal("unused collector not closed")
	}
	c, _ = testNetCollector(t, time.Second)
	c.write = func([]byte) error { return io.ErrShortWrite }
	c.begin(time.Now())
	<-c.done
	if err := c.stop(); err == nil || !strings.Contains(err.Error(), "output_unavailable") {
		t.Fatal(err)
	}
}

func TestDiagNetOptions(t *testing.T) {
	base := []string{"-plaintext", "-self=127.0.0.1:9000", "-remotes=127.0.0.1:9000", "-output=x.binpb", "-f=0", "-diag-net"}
	if _, err := ParseOptions(base, io.Discard); err == nil {
		t.Fatal("accepted no interval")
	}
	opts, err := ParseOptions(append(base, "-diag-interval=1s"), io.Discard)
	if netDiagSupported {
		if err != nil || !opts.diagNet {
			t.Fatal(err)
		}
	} else if err == nil {
		t.Fatal("accepted on unsupported platform")
	}
}

func BenchmarkNetDiagRound(b *testing.B) {
	peers := make([]string, 13)
	for i := range peers {
		peers[i] = fmt.Sprintf("192.0.2.%d:9000", i+1)
	}
	var representative strings.Builder
	representative.WriteString(procHeader)
	for i := 2; i <= 13; i++ {
		representative.WriteString(socketFixture("192.0.2.1:45001", fmt.Sprintf("192.0.2.%d:9000", i), 1, uint64(i)))
		representative.WriteString(socketFixture("192.0.2.1:9000", fmt.Sprintf("192.0.2.%d:45002", i), 1, uint64(i+13)))
	}
	for _, size := range []string{"24-sockets", "entry-cap", "byte-cap"} {
		b.Run(size, func(b *testing.B) {
			data := representative.String()
			if size != "24-sockets" {
				data += strings.Repeat(socketFixture("192.0.2.1:1234", "203.0.113.1:80", 1, 2), netFileRows-24)
			}
			if size == "byte-cap" {
				data += strings.Repeat(" ", netFileBytes-len(data)-1) + "\n"
			}
			c, err := makeNetCollector(1, peers, time.Second, nil, func([]byte) error { return nil }, func() {})
			if err != nil {
				b.Fatal(err)
			}
			raw := []byte(data)
			c.read = func(ctx context.Context, name string) ([]byte, error) {
				if name == "tcp" {
					return raw, nil
				}
				return fixtureRead(ctx, name)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if !c.collect(time.Now(), 0, "periodic", nil) {
					b.Fatal(c.err)
				}
			}
		})
	}
}

func TestRunKeepsReportOnIncompleteNetworkDiagnostics(t *testing.T) {
	for _, mode := range []string{"read-timeout", "write-timeout", "output-failure", "unavailable-sink", "resource-stop"} {
		t.Run(mode, func(t *testing.T) {
			peer := addresses(t, 1)[0]
			path := filepath.Join(t.TempDir(), "report.binpb")
			opts, err := ParseOptions([]string{"-plaintext", "-self=" + peer, "-remotes=" + peer, "-f=0", "-output=" + path, "-time=100ms", "-diag-interval=1h"}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			c, sink := testNetCollector(t, time.Hour)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			switch mode {
			case "output-failure", "unavailable-sink":
				c.sinkUnavailable = mode == "unavailable-sink"
				c.write = func(line []byte) error {
					if !bytes.Contains(line, []byte(`"kind":"terminal"`)) {
						return io.ErrShortWrite
					}
					return sink.write(line)
				}
			case "write-timeout":
				c.write = func(line []byte) error { once.Do(func() { close(entered); <-release }); return sink.write(line) }
			default:
				c.read = func(ctx context.Context, name string) ([]byte, error) {
					once.Do(func() { close(entered); <-release })
					return fixtureRead(ctx, name)
				}
			}
			c.joinTimeout = 20 * time.Millisecond
			opts.diagNet = true
			opts.makeNet = func(uint32, []string, time.Duration) (*netCollector, error) { return c, nil }
			if mode == "resource-stop" {
				opts.limits = resourceLimits{goroutines: 1000}
				var reads atomic.Int32
				opts.readResources = func() (resources, error) {
					if reads.Add(1) == 1 {
						return resources{goroutines: 10}, nil
					}
					return resources{goroutines: 1001}, nil
				}
			}
			var runErr error
			output := captureStderr(t, func() { runErr = Run(t.Context(), opts) })
			if mode != "output-failure" && mode != "unavailable-sink" {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("collector never entered")
				}
				close(release)
			}
			select {
			case <-c.done:
			case <-time.After(time.Second):
				t.Fatal("collector did not exit")
			}
			if mode == "resource-stop" {
				if runErr == nil || !strings.HasPrefix(runErr.Error(), "brb bench: resource stop (goroutines)") {
					t.Fatal(runErr)
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("resource stop wrote report", err)
				}
			} else {
				if runErr != nil {
					t.Fatal("diagnostic failure invalidated benchmark", runErr)
				}
				report, err := benchkit.LoadReport(path)
				if err != nil || len(report.GetResults()) != 1 || report.GetResults()[0].GetTotalOps() == 0 {
					t.Fatal("missing successful benchmark report", err)
				}
			}
			records := sink.snapshot()
			last := records[len(records)-1]
			want := "shutdown_timeout"
			if mode == "output-failure" || mode == "unavailable-sink" {
				want = "output_unavailable"
			}
			mainStatus := want
			if mode == "unavailable-sink" {
				mainStatus = "unavailable_sink"
			}
			if strings.Count(output, "diag net-status ") != 1 || !strings.Contains(output, "diag net-status status="+mainStatus+"\n") {
				t.Fatal("missing or repeated main shutdown status", output)
			}
			if last.Terminal == nil || last.Terminal.Status != want {
				t.Fatal("missing terminal status", last)
			}
			for _, r := range records {
				if r.Phase == "final" && r.Kind == "round" {
					t.Fatal("fabricated final round")
				}
			}
		})
	}
}

// The shared synthetic fixture checks that Go output remains compatible with
// the independent Python validator. It contains no real endpoint information.
func TestNetSchemaFixture(t *testing.T) {
	var output bytes.Buffer
	start := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for node := uint32(1); node <= 2; node++ {
		c, _ := testNetCollector(t, time.Second)
		c.self = node
		c.membership, _ = newNetMembership(node, []string{"192.0.2.1:9000", "192.0.2.2:9000"})
		c.write = func(line []byte) error {
			_, err := fmt.Fprintf(&output, "2026/10/29 12:00:00 [host%d:9000] %s", node, line)
			return err
		}
		c.read = func(ctx context.Context, name string) ([]byte, error) {
			if node == 2 && name == "tcp" {
				return []byte(procHeader + socketFixture("192.0.2.2:9000", "192.0.2.1:45001", 1, 99)), nil
			}
			return fixtureRead(ctx, name)
		}
		for round, phase := range []string{"initial", "periodic", "final"} {
			at := start.Add(time.Duration(round) * time.Second)
			c.now = func() time.Time { return at }
			if !c.collect(start, uint64(round), phase, nil) {
				t.Fatal(c.err)
			}
		}
		fmt.Fprintf(&output, "2026/10/29 12:00:00 [host%d:9000] brb bench: node %d diag net-status status=ok\n", node, node)
	}
	path := "../../tools/testdata/diag-net-v1.log"
	if os.Getenv("BRB_UPDATE_NET_FIXTURE") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, output.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, output.Bytes()) {
		t.Fatal("Go output differs from shared validator fixture; review before updating it")
	}
}

func TestNetCollectorCapsAndHistory(t *testing.T) {
	t.Run("socket cap", func(t *testing.T) {
		c, sink := testNetCollector(t, time.Second)
		c.read = func(ctx context.Context, name string) ([]byte, error) {
			if name == "tcp" {
				return []byte(procHeader + strings.Repeat(socketFixture("192.0.2.1:45001", "192.0.2.2:9000", 1, 42), netSockets+1)), nil
			}
			return fixtureRead(ctx, name)
		}
		if !c.collect(time.Now(), 0, "initial", nil) {
			t.Fatal(c.err)
		}
		r := sink.snapshot()
		v := r[len(r)-1].Summary
		if len(r) != netSockets+1 || v.Sockets != netSockets || v.Sources[0].Status != "socket_limit" {
			t.Fatal(v)
		}
	})
	t.Run("history cap", func(t *testing.T) {
		c, sink := testNetCollector(t, time.Second)
		for port := uint16(10000); len(c.history) < netSockets; port++ {
			c.history[socketKey{1, 2, port}] = socketLife{inode: 1, generation: 1}
		}
		c.collect(time.Now(), 0, "initial", nil)
		r := sink.snapshot()
		v := r[len(r)-1].Summary
		if len(c.history) != netSockets || len(v.Problems) != 1 || v.Problems[0] != "history_limit" || r[0].Socket.Generation != 0 {
			t.Fatal(v)
		}
	})
	t.Run("line cap", func(t *testing.T) {
		peers := make([]string, netSockets/2+1)
		for i := range peers {
			peers[i] = fmt.Sprintf("192.0.2.%d:9000", i+1)
		}
		c, sink := testNetCollector(t, time.Second)
		c.membership, _ = newNetMembership(1, peers)
		if c.collect(time.Now(), 0, "initial", nil) || !strings.Contains(c.err.Error(), "output_limit") || len(sink.snapshot()) != 0 {
			t.Fatalf("unbounded output %v", c.err)
		}
	})
}

func TestNetMissingSocketDoesNotFabricateReconnectAfterReadFailure(t *testing.T) {
	c, sink := testNetCollector(t, time.Second)
	start := time.Now()
	c.collect(start, 0, "initial", nil)
	c.read = func(ctx context.Context, name string) ([]byte, error) {
		if name == "tcp" {
			return nil, errors.New("read failed")
		}
		return fixtureRead(ctx, name)
	}
	c.collect(start, 1, "periodic", nil)
	c.read = fixtureRead
	c.collect(start, 2, "periodic", nil)
	r := sink.snapshot()
	s := r[len(r)-2].Socket
	if s.Generation != 1 || s.Reconnect {
		t.Fatal(s)
	}
	c.read = func(ctx context.Context, name string) ([]byte, error) {
		if name == "tcp" {
			return []byte(procHeader), nil
		}
		return fixtureRead(ctx, name)
	}
	c.collect(start, 3, "periodic", nil)
	c.read = fixtureRead
	c.collect(start, 4, "final", nil)
	r = sink.snapshot()
	s = r[len(r)-2].Socket
	if s.Generation != 2 || !s.Reconnect {
		t.Fatal(s)
	}
}

func TestNetFinalRoundMissedSlots(t *testing.T) {
	for _, slot := range []uint64{0, 1, 2, 3} {
		t.Run(fmt.Sprint(slot), func(t *testing.T) {
			c, sink := testNetCollector(t, time.Hour)
			start := time.Now()
			var offset atomic.Int64
			c.now = func() time.Time { return start.Add(time.Duration(offset.Load())) }
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			c.read = func(ctx context.Context, name string) ([]byte, error) {
				once.Do(func() { close(entered); <-release })
				return fixtureRead(ctx, name)
			}
			c.begin(start)
			<-entered
			stopped := make(chan error, 1)
			go func() { stopped <- c.stop() }()
			<-c.stopRequest
			offset.Store(int64(time.Duration(slot) * time.Hour))
			close(release)
			if err := <-stopped; err != nil {
				t.Fatal(err)
			}
			var missed []*netMissed
			for _, r := range sink.snapshot() {
				if r.Missed != nil {
					if r.Phase != "final" {
						t.Fatal(r)
					}
					missed = append(missed, r.Missed)
				}
			}
			if slot <= 1 {
				if len(missed) != 0 {
					t.Fatal("final round counted its own slot", missed)
				}
			} else {
				if len(missed) != 1 || *missed[0] != (netMissed{1, slot - 1, slot - 1}) {
					t.Fatal(missed)
				}
			}
		})
	}
	if netJoinTimeout != 2*time.Second {
		t.Fatal("unexpected shutdown budget", netJoinTimeout)
	}
	if m := missedNetSlots(2, 5); m == nil || *m != (netMissed{3, 4, 2}) {
		t.Fatal(m)
	}
}

func TestNetTerminalSchemaFixture(t *testing.T) {
	c, _ := testNetCollector(t, time.Second)
	var output bytes.Buffer
	var writes int
	c.write = func(line []byte) error {
		writes++
		if writes == 4 {
			return io.ErrShortWrite
		} // periodic footer is unavailable
		_, err := fmt.Fprintf(&output, "2026/10/29 12:00:00 [host1:9000] %s", line)
		return err
	}
	start := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	at := start
	c.now = func() time.Time { return at }
	if !c.collect(start, 0, "initial", nil) {
		t.Fatal(c.err)
	}
	at = start.Add(time.Second)
	if c.collect(start, 1, "periodic", nil) {
		t.Fatal("expected fixture output failure")
	}
	c.logTerminal(start)
	fmt.Fprintln(&output, "2026/10/29 12:00:00 [host1:9000] brb bench: node 1 diag net-status status=output_unavailable")
	path := "../../tools/testdata/diag-net-terminal-v1.log"
	if os.Getenv("BRB_UPDATE_NET_FIXTURE") == "1" {
		if err := os.WriteFile(path, output.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, output.Bytes()) {
		t.Fatal("Go terminal output differs from shared fixture")
	}
}

func TestSamplerLogsNetShutdownStatusOnce(t *testing.T) {
	for _, status := range []string{"ok", "not_started", "shutdown_timeout", "output_unavailable", "output_limit", "unavailable_sink"} {
		t.Run(status, func(t *testing.T) {
			c, _ := testNetCollector(t, time.Hour)
			entered, release := make(chan struct{}), make(chan struct{})
			switch status {
			case "shutdown_timeout":
				var once sync.Once
				c.read = func(ctx context.Context, name string) ([]byte, error) {
					once.Do(func() { close(entered); <-release })
					return fixtureRead(ctx, name)
				}
				c.joinTimeout = 20 * time.Millisecond
			case "output_unavailable", "unavailable_sink":
				c.write = func([]byte) error { return io.ErrShortWrite }
				c.sinkUnavailable = status == "unavailable_sink"
			case "output_limit":
				peers := make([]string, netSockets/2+1)
				for i := range peers {
					peers[i] = fmt.Sprintf("192.0.2.%d:9000", i+1)
				}
				c.membership, _ = newNetMembership(1, peers)
			}
			s, _ := testSampler(t, time.Hour, resourceLimits{}, func() resources { return resources{goroutines: 1} })
			s.net = c
			var output strings.Builder
			s.printf = func(format string, args ...any) { fmt.Fprintf(&output, format, args...) }
			if status != "not_started" {
				s.begin(time.Now())
			}
			if status == "shutdown_timeout" {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("collector not entered")
				}
			} else if status != "ok" && status != "not_started" {
				select {
				case <-c.done:
				case <-time.After(time.Second):
					t.Fatal("collector did not fail")
				}
			}
			s.stop()
			// This ordinary line is available before a blocked worker is released.
			s.logNetStatus()
			s.logNetStatus()
			want := fmt.Sprintf("brb bench: node %d diag net-status status=%s\n", s.selfID, status)
			if strings.Count(output.String(), "diag net-status ") != 1 || !strings.Contains(output.String(), want) {
				t.Fatal(output.String())
			}
			if status == "shutdown_timeout" {
				close(release)
				select {
				case <-c.done:
				case <-time.After(time.Second):
					t.Fatal("collector did not exit")
				}
			}
		})
	}
}

func TestNoNetShutdownLineWhenDisabled(t *testing.T) {
	s := &sampler{printf: func(string, ...any) { t.Fatal("unexpected network status") }}
	s.logNetStatus()
}
