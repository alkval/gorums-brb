package main

import (
	"context"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"
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
	if opts.id != 2 || opts.f != 1 || len(opts.peers) != 4 || opts.peers[1] != "127.0.0.1:7002" || opts.broadcast != "hello" {
		t.Fatalf("unexpected options: %+v", opts)
	}
}

func TestRejectInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
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
