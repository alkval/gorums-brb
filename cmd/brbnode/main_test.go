package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestParseOptions(t *testing.T) {
	opts, err := parseOptions([]string{
		"-id", "2", "-f", "1",
		"-peers", "127.0.0.1:7001, 127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004",
		"-broadcast", "hello",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.id != 2 || opts.f != 1 || len(opts.peers) != 4 || opts.peers[1] != "127.0.0.1:7002" || opts.broadcast != "hello" || opts.count != 1 {
		t.Fatalf("unexpected options: %+v", opts)
	}
}

func TestParseBroadcastCount(t *testing.T) {
	opts, err := parseOptions([]string{
		"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001",
		"-broadcast", "hello", "-count", "3",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.count != 3 {
		t.Fatalf("count = %d, want 3", opts.count)
	}
}

func TestParsePeriodicWorkload(t *testing.T) {
	opts, err := parseOptions([]string{
		"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001",
		"-payload-size", "256", "-count", "3", "-interval", "100ms",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.payloadSize != 256 || opts.count != 3 || opts.interval != 100*time.Millisecond {
		t.Fatalf("unexpected workload options: %+v", opts)
	}
}

func TestRejectInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"negative interval", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "-broadcast", "hello", "-interval", "-1ms"}},
		{"invalid interval", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "-broadcast", "hello", "-interval", "fast"}},
		{"interval without payload", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "-interval", "1s"}},
		{"negative payload size", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "-payload-size", "-1"}},
		{"oversized payload", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "-payload-size", "1048577"}},
		{"two payload sources", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "-payload-size", "256", "-broadcast", "hello"}},
		{"zero count", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "-broadcast", "hello", "-count", "0"}},
		{"negative count", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "-broadcast", "hello", "-count", "-1"}},
		{"count without broadcast", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "-count", "3"}},
		{"missing peers", []string{"-id", "1", "-f", "0"}},
		{"missing ID", []string{"-f", "0", "-peers", "127.0.0.1:7001"}},
		{"ID outside membership", []string{"-id", "2", "-f", "0", "-peers", "127.0.0.1:7001"}},
		{"missing fault bound", []string{"-id", "1", "-peers", "127.0.0.1:7001"}},
		{"too many faults", []string{"-id", "1", "-f", "1", "-peers", "127.0.0.1:7001"}},
		{"duplicate endpoint", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001,127.0.0.1:07001"}},
		{"missing port", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1"}},
		{"empty host", []string{"-id", "1", "-f", "0", "-peers", ":7001"}},
		{"nonnumeric port", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:abc"}},
		{"zero port", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:0"}},
		{"large port", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:65536"}},
		{"wildcard host", []string{"-id", "1", "-f", "0", "-peers", "0.0.0.0:7001"}},
		{"IPv6 wildcard", []string{"-id", "1", "-f", "0", "-peers", "[::]:7001"}},
		{"positional argument", []string{"-id", "1", "-f", "0", "-peers", "127.0.0.1:7001", "extra"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseOptions(tc.args, io.Discard); err == nil {
				t.Fatal("accepted invalid options")
			}
		})
	}
}

// Capture log lines without sharing a bytes.Buffer across goroutines.
type logLines chan string

func (lines logLines) Write(p []byte) (int, error) {
	lines <- string(p)
	return len(p), nil
}

// Wait for the node to finish even if the test fails before its normal shutdown.
func startTestNode(t *testing.T, opts options) (logLines, context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	lines := make(logLines, 10)
	done := make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		done <- run(ctx, opts, log.New(lines, "", 0))
		close(stopped)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			t.Error("node did not stop during test cleanup")
		}
	})
	return lines, cancel, done
}

func TestRunStartsAndStops(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	opts, err := parseOptions([]string{"-id", "1", "-f", "0", "-peers", addr}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	lines, cancel, done := startTestNode(t, opts)
	select {
	case line := <-lines:
		if !strings.Contains(line, "node 1 listening on "+addr) {
			t.Fatalf("unexpected startup log: %s", line)
		}
	case err := <-done:
		t.Fatalf("node exited before startup: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("startup timed out")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown timed out")
	}
	// Shutdown must release the listening port.
	listener, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("port not released: %v", err)
	}
	_ = listener.Close()
}

func TestRunRejectsOccupiedPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	opts, err := parseOptions([]string{"-id", "1", "-f", "0", "-peers", listener.Addr().String()}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), opts, log.New(io.Discard, "", 0)); err == nil {
		t.Fatal("accepted occupied port")
	}
}

func TestRunBroadcastsToSelf(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	opts, err := parseOptions([]string{
		"-id", "1", "-f", "0", "-peers", addr, "-broadcast", "hello",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	lines, cancel, done := startTestNode(t, opts)
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case line := <-lines:
			if !strings.Contains(line, `node 1 delivered origin=1 sequence=1 value="hello"`) {
				continue
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown timed out")
			}
			return
		case err := <-done:
			t.Fatalf("node exited before delivery: %v", err)
		case <-timeout.C:
			t.Fatal("delivery timed out")
		}
	}
}

func TestRunStopsWhileWaitingForPeers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	// Reserve a second endpoint without serving Gorums on it.
	missingPeer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer missingPeer.Close()
	opts, err := parseOptions([]string{
		"-id", "1", "-f", "0", "-peers", addr + "," + missingPeer.Addr().String(),
		"-broadcast", "hello",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	lines, cancel, done := startTestNode(t, opts)
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case line := <-lines:
			if !strings.Contains(line, "waiting for all 2 peers") {
				continue
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown while waiting timed out")
			}
			return
		case err := <-done:
			t.Fatalf("node exited before waiting: %v", err)
		case <-timeout.C:
			t.Fatal("startup timed out")
		}
	}
}

type logWriter func(string)

func (write logWriter) Write(p []byte) (int, error) {
	write(string(p))
	return len(p), nil
}

func TestRunStopsBetweenBroadcasts(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	opts, err := parseOptions([]string{
		"-id", "1", "-f", "0", "-peers", addr,
		"-broadcast", "hello", "-count", "3",
		"-interval", "1h",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	submitted := 0
	// Cancel while the node is waiting for its next submission.
	logger := log.New(logWriter(func(line string) {
		if strings.Contains(line, "submitted origin=1") {
			submitted++
			time.AfterFunc(10*time.Millisecond, cancel)
		}
	}), "", 0)
	if err := run(ctx, opts, logger); err != nil {
		t.Fatalf("requested shutdown reported a failure: %v", err)
	}
	if submitted != 1 {
		t.Fatalf("submitted %d broadcasts, want 1", submitted)
	}
}

func TestRunSpacesSubmissions(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	opts, err := parseOptions([]string{
		"-id", "1", "-f", "0", "-peers", addr,
		"-payload-size", "256", "-count", "3", "-interval", "50ms",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var submitted []time.Time
	// Only submission logs touch this slice, in the goroutine running run.
	logger := log.New(logWriter(func(line string) {
		if strings.Contains(line, "submitted origin=1") {
			if !strings.Contains(line, "bytes=256") {
				t.Errorf("unexpected payload log: %s", line)
			}
			submitted = append(submitted, time.Now())
			if len(submitted) == opts.count {
				cancel()
			}
		}
	}), "", 0)
	if err := run(ctx, opts, logger); err != nil {
		t.Fatal(err)
	}
	if len(submitted) != opts.count {
		t.Fatalf("submitted %d broadcasts, want %d", len(submitted), opts.count)
	}
	for i := 1; i < len(submitted); i++ {
		if gap := submitted[i].Sub(submitted[i-1]); gap < opts.interval {
			t.Errorf("submission gap = %s, want at least %s", gap, opts.interval)
		}
	}
}

func TestRunGeneratedPayloadContents(t *testing.T) {
	for _, size := range []int{1, 64, 65, 256, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			// A real receiving node exposes delivered bytes without changing the
			// executable's logging or adding a test hook to production code.
			var addresses []string
			var listeners []net.Listener
			for range 2 {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				listeners = append(listeners, listener)
				t.Cleanup(func() { _ = listener.Close() })
				addresses = append(addresses, listener.Addr().String())
			}
			for _, listener := range listeners {
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			}
			peers := gorums.WithNodeList(addresses)
			server := gorums.NewServer(
				gorums.WithAddr(addresses[1]),
				gorums.WithPeers(2, peers, gorums.WithGRPCDialOptions(
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
				)),
			)
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			t.Cleanup(func() {
				cancel()
				server.Stop()
			})
			deliveries := make(chan brb.Delivery, 2)
			if _, err := brb.NewReplica(ctx, server, 0, func(d brb.Delivery) { deliveries <- d }); err != nil {
				t.Fatal(err)
			}
			serveErr := make(chan error, 1)
			go func() { serveErr <- server.ListenAndServe() }()
			opts, err := parseOptions([]string{
				"-id", "1", "-f", "0", "-peers", strings.Join(addresses, ","),
				"-payload-size", fmt.Sprint(size), "-count", "2",
			}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			_, stopSender, senderDone := startTestNode(t, opts)
			seen := make(map[uint64]bool)
			for range 2 {
				select {
				case d := <-deliveries:
					if d.ID.Origin != 1 || d.ID.Sequence < 1 || d.ID.Sequence > 2 || seen[d.ID.Sequence] {
						t.Fatalf("unexpected delivery ID: %+v", d.ID)
					}
					seen[d.ID.Sequence] = true
					if len(d.Value) != size {
						t.Fatalf("delivered %d bytes, want %d", len(d.Value), size)
					}
					for i, b := range d.Value {
						if b != 'x' {
							t.Fatalf("payload byte %d = %d, want 'x'", i, b)
						}
					}
				case err := <-senderDone:
					t.Fatalf("sender stopped before delivery: %v", err)
				case err := <-serveErr:
					t.Fatalf("receiver stopped before delivery: %v", err)
				case <-ctx.Done():
					t.Fatal("payload delivery timed out")
				}
			}
			stopSender()
			select {
			case err := <-senderDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("sender shutdown timed out")
			}
		})
	}
}

func TestRunStopsBeforeInitialBroadcast(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	opts, err := parseOptions([]string{
		"-id", "1", "-f", "0", "-peers", addr, "-broadcast", "hello",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Cancel at the startup boundary, without relying on a timing delay.
	logger := log.New(logWriter(func(line string) {
		if strings.Contains(line, "waiting for all 1 peers") {
			cancel()
		}
	}), "", 0)
	if err := run(ctx, opts, logger); err != nil {
		t.Fatalf("requested shutdown reported a failure: %v", err)
	}
}
