package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/relab/gorums/benchkit"
)

func TestParseOptions(t *testing.T) {
	base := []string{"-self=127.0.0.1:9000", "-remotes=127.0.0.1:9000", "-f=0", "-output=result.binpb"}
	opts, err := parseOptions(base, io.Discard)
	if err != nil || opts.selfID != 1 {
		t.Fatalf("valid options: %+v, %v", opts, err)
	}
	for _, extra := range []string{
		"-workers=0", "-payload=-1", "-payload=1048577", "-time=0s", "-rate=-1",
		"-f=1", "-output=", "-self=0.0.0.0:9000", "-self=127.0.0.1:0",
		"-self=127.0.0.1:9001", "-remotes=127.0.0.1:9000,127.0.0.1:9000",
		"-benchmarks=Other", "-stream-mode=dedup", "-cpuprofile=cpu.out",
		"-rate-step=1", "-call-timeout=1s", "-fault-kill-after=1s", "extra",
	} {
		t.Run(extra, func(t *testing.T) {
			if _, err := parseOptions(append(append([]string{}, base...), extra), io.Discard); err == nil {
				t.Fatal("accepted invalid or unsupported options")
			}
		})
	}
}

func addresses(t *testing.T, count int) []string {
	t.Helper()
	var listeners []net.Listener
	var result []string
	for range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		t.Cleanup(func() { _ = listener.Close() })
		result = append(result, listener.Addr().String())
	}
	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func checkReport(t *testing.T, path string, nodes, workers, payload int) {
	t.Helper()
	report, err := benchkit.LoadReport(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.GetResults()) != 1 {
		t.Fatalf("got %d results, want 1", len(report.GetResults()))
	}
	r := report.GetResults()[0]
	c := r.GetConfig()
	if c.GetName() != benchmarkName || int(c.GetNumNodes()) != nodes || int(c.GetWorkers()) != workers || int(c.GetPayload()) != payload {
		t.Fatalf("incorrect report metadata: %v", c)
	}
	if r.GetTotalOps() == 0 || r.GetFailedOps() != 0 || r.GetThroughput() <= 0 || len(r.Percentiles(0.95)) != 1 {
		t.Fatalf("invalid measurement: ops=%d failed=%d throughput=%f", r.GetTotalOps(), r.GetFailedOps(), r.GetThroughput())
	}
}

func TestLocalDeliveryAndReport(t *testing.T) {
	for _, size := range []int{0, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			peer := addresses(t, 1)[0]
			path := filepath.Join(t.TempDir(), "result.binpb")
			opts, err := parseOptions([]string{"-self=" + peer, "-remotes=" + peer, "-f=0",
				"-output=" + path, "-time=50ms", "-workers=3", fmt.Sprintf("-payload=%d", size)}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := run(ctx, opts); err != nil {
				t.Fatal(err)
			}
			checkReport(t, path, 1, 3, size)
			listener, err := net.Listen("tcp", peer)
			if err != nil {
				t.Fatalf("listener leaked: %v", err)
			}
			_ = listener.Close()
		})
	}
}

func TestFixedRateStopsAtMeasurementDeadline(t *testing.T) {
	peer := addresses(t, 1)[0]
	path := filepath.Join(t.TempDir(), "result.binpb")
	opts, err := parseOptions([]string{"-self=" + peer, "-remotes=" + peer,
		"-f=0", "-output=" + path, "-time=200ms", "-workers=1", "-rate=1"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := run(ctx, opts); err != nil {
		t.Fatal(err)
	}
	checkReport(t, path, 1, 1, 0)
	report, err := benchkit.LoadReport(path)
	if err != nil {
		t.Fatal(err)
	}
	r := report.GetResults()[0]
	// The first operation is immediate. The next slot is at one second,
	// outside the 200 ms window, and must not extend the measurement.
	if r.GetTotalOps() != 1 {
		t.Fatalf("completed %d operations, want only the initial operation", r.GetTotalOps())
	}
	if elapsed := time.Duration(r.GetTotalTime()); elapsed < opts.Duration || elapsed >= 750*time.Millisecond {
		t.Fatalf("measurement lasted %s, want about %s, not the next pacing slot", elapsed, opts.Duration)
	}
}

func TestFourNodesProduceReports(t *testing.T) {
	peers := addresses(t, 4)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	done := make(chan error, 4)
	paths := make([]string, 4)
	for i, peer := range peers {
		paths[i] = filepath.Join(t.TempDir(), "result.binpb")
		opts, err := parseOptions([]string{"-self=" + peer, "-remotes=" + strings.Join(peers, ","),
			"-f=1", "-output=" + paths[i], "-time=200ms", "-workers=2", "-payload=256"}, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		go func() { done <- run(ctx, opts) }()
	}
	for range 4 {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	if !t.Failed() {
		for _, path := range paths {
			checkReport(t, path, 4, 2, 256)
		}
	}
}

func TestCancellationWithMissingPeer(t *testing.T) {
	peers := addresses(t, 2)
	opts, err := parseOptions([]string{"-self=" + peers[0], "-remotes=" + strings.Join(peers, ","),
		"-f=0", "-output=" + filepath.Join(t.TempDir(), "result.binpb")}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := run(ctx, opts); err == nil {
		t.Fatal("missing-peer startup reported success")
	}
}
