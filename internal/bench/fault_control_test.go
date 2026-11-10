package bench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/relab/gorums"
	"github.com/relab/gorums/benchkit"
)

func TestIdleNodeOptions(t *testing.T) {
	peers := "127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003"
	base := []string{"-plaintext", "-self=127.0.0.1:9000", "-remotes=" + peers, "-output=result.binpb"}
	opts, err := ParseOptions(append(append([]string{}, base...), "-idle-node=4"), io.Discard)
	if err != nil || opts.idleNode != 4 || opts.failFast {
		t.Fatalf("idle control: %+v, %v", opts, err)
	}
	for _, extra := range [][]string{
		{"-idle-node=5"},
		{"-idle-node=4", "-fault=silent"},
		{"-idle-node=4", "-fault=wrong-echo"},
		{"-idle-node=3", "-fault-kill-after=1s", "-time=2s"},
	} {
		t.Run(strings.Join(extra, " "), func(t *testing.T) {
			if _, err := ParseOptions(append(append([]string{}, base...), extra...), io.Discard); err == nil {
				t.Fatal("accepted an invalid idle-node combination")
			}
		})
	}
}

// runGroup runs a four-node benchmark with the extra arguments on every node
// and returns the captured log, the report paths and each node's result.
func runGroup(t *testing.T, extra ...string) (logged string, paths []string, errs []error) {
	t.Helper()
	peers := addresses(t, 4)
	paths, errs = make([]string, 4), make([]error, 4)
	logged = captureStderr(t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		done := make(chan struct{}, 4)
		for i, peer := range peers {
			paths[i] = filepath.Join(t.TempDir(), "result.binpb")
			args := append([]string{"-plaintext", "-self=" + peer, "-remotes=" + strings.Join(peers, ","),
				"-f=1", "-output=" + paths[i], "-time=300ms", "-payload=64"}, extra...)
			opts, err := ParseOptions(args, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			go func() { errs[i] = Run(ctx, opts); done <- struct{}{} }()
		}
		for range 4 {
			<-done
		}
	})
	return logged, paths, errs
}

func loggedCount(t *testing.T, logged, format string, id int) uint64 {
	t.Helper()
	m := regexp.MustCompile(regexp.QuoteMeta(fmt.Sprintf(format, id))+`(\d+)`).FindAllStringSubmatch(logged, -1)
	if len(m) != 1 {
		t.Fatalf("node %d: %d lines matching %q in:\n%s", id, len(m), format, logged)
	}
	v, err := strconv.ParseUint(m[0][1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// The idle control replica takes part in the protocol, so the others complete
// their broadcasts and it delivers them, but it generates no workload and
// writes no report. The active replicas label their reports with it.
func TestIdleControlRun(t *testing.T) {
	logged, paths, errs := runGroup(t, "-fail-fast", "-idle-node=4")
	for i, err := range errs {
		if err != nil {
			t.Fatalf("node %d: %v", i+1, err)
		}
	}
	for i, path := range paths[:3] {
		report, err := benchkit.LoadReport(path)
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("node %d f=1 auth=none fail-fast=true idle-node=4", i+1)
		if report.GetLabel() != want || report.GetResults()[0].GetTotalOps() == 0 {
			t.Fatalf("node %d: label %q ops %d, want %q and completed broadcasts", i+1, report.GetLabel(),
				report.GetResults()[0].GetTotalOps(), want)
		}
	}
	if _, err := os.Stat(paths[3]); !os.IsNotExist(err) {
		t.Fatalf("the idle replica wrote a report (stat: %v)", err)
	}
	if !strings.Contains(logged, "brb bench: node 4 idle control: no workload, no report\n") {
		t.Fatalf("missing idle control line in:\n%s", logged)
	}
	if n := loggedCount(t, logged, "brb bench: node %d traffic phase=propose calls=", 4); n != 0 {
		t.Fatalf("the idle replica proposed %d broadcasts", n)
	}
	if n := loggedCount(t, logged, "brb bench: node %d traffic phase=echo calls=", 4); n == 0 {
		t.Fatal("the idle replica sent no ECHO: it did not take part")
	}
	for id := 1; id <= 4; id++ {
		if n := loggedCount(t, logged, "brb bench: node %d diag conflicting-echoes count=", id); n != 0 {
			t.Fatalf("node %d: %d conflicting ECHOs in a fault-free run", id, n)
		}
	}
	var total uint64
	if _, err := fmt.Sscanf(logged[strings.Index(logged, "brb bench: node 4 deliveries "):],
		"brb bench: node 4 deliveries window_ns=300000000 in_window=%d total=%d", new(uint64), &total); err != nil || total == 0 {
		t.Fatalf("idle replica deliveries: total=%d, %v", total, err)
	}
}

// Correct replicas count the wrong-value ECHOs they accept, which separates
// this fault from a silent replica in the logs.
func TestWrongEchoIsCounted(t *testing.T) {
	logged, paths, errs := runGroup(t, "-fault=wrong-echo")
	for i, err := range errs {
		if err != nil {
			t.Fatalf("node %d: %v", i+1, err)
		}
	}
	for id := 1; id <= 3; id++ {
		if _, err := benchkit.LoadReport(paths[id-1]); err != nil {
			t.Fatal(err)
		}
		if n := loggedCount(t, logged, "brb bench: node %d diag conflicting-echoes count=", id); n == 0 {
			t.Fatalf("node %d counted no conflicting ECHO under -fault=wrong-echo", id)
		}
	}
}

// With four replicas the equivocating origin's value A reaches an ECHO quorum,
// so every correct replica delivers it and logs the same digest.
func TestEquivocationLogsDeliveredValue(t *testing.T) {
	logged, _, errs := runGroup(t, "-fault=equivocate")
	for i, err := range errs {
		if err != nil {
			t.Fatalf("node %d: %v", i+1, err)
		}
	}
	sum := sha256.Sum256([]byte("A"))
	for id := 1; id <= 3; id++ {
		want := fmt.Sprintf("brb bench: node %d watched origin=4 sequence=1 delivered=true sha256=%s\n", id, hex.EncodeToString(sum[:]))
		if !strings.Contains(logged, want) {
			t.Fatalf("missing %q in:\n%s", want, logged)
		}
	}
}

// An idle replica whose server stops during the workload is no longer taking
// part, so its run fails and it does not report a successful idle control.
func TestIdleControlFailsWhenServerStops(t *testing.T) {
	peers := addresses(t, 4)
	errs := make([]error, 4)
	logged := captureStderr(t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		done := make(chan struct{}, 4)
		for i, peer := range peers {
			opts, err := ParseOptions([]string{"-plaintext", "-self=" + peer, "-remotes=" + strings.Join(peers, ","),
				"-f=1", "-output=" + filepath.Join(t.TempDir(), "result.binpb"), "-time=2s", "-payload=64",
				"-fail-fast", "-idle-node=4"}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if i == 3 {
				opts.idleStarted = func(server *gorums.Server) { go server.Stop() }
			}
			go func() { errs[i] = Run(ctx, opts); done <- struct{}{} }()
		}
		for range 4 {
			<-done
		}
	})
	if !errors.Is(errs[3], errServerStopped) {
		t.Fatalf("idle replica after its server stopped: %v, want %v", errs[3], errServerStopped)
	}
	if strings.Contains(logged, "node 4 idle control: no workload, no report") {
		t.Fatalf("the stopped idle replica reported a successful control:\n%s", logged)
	}
}

func TestIdleWait(t *testing.T) {
	serveErr := make(chan error, 1)
	if err := idleWait(t.Context(), time.Millisecond, serveErr); err != nil {
		t.Fatalf("full period: %v", err)
	}
	serveErr <- errors.New("listener closed")
	if err := idleWait(t.Context(), time.Minute, serveErr); !errors.Is(err, errServerStopped) ||
		err.Error() != "brb bench: server stopped: listener closed" {
		t.Fatalf("server exit: %v", err)
	}
	serveErr <- nil
	if err := idleWait(t.Context(), time.Minute, serveErr); !errors.Is(err, errServerStopped) {
		t.Fatalf("server exit without an error: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := idleWait(ctx, time.Minute, serveErr); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestDeliveryTrackerWatch(t *testing.T) {
	tracker := newTestTracker(time.Now)
	tracker.watch = &brb.BroadcastID{Origin: 4, Sequence: 1}
	tracker.open(time.Second)
	tracker.delivered(brb.Delivery{ID: brb.BroadcastID{Origin: 4, Sequence: 2}, Value: []byte("other")})
	logged := captureStderr(t, func() { logDeliveries(1, tracker, time.Second) })
	if !strings.Contains(logged, "brb bench: node 1 watched origin=4 sequence=1 delivered=false sha256=none\n") {
		t.Fatalf("before delivery: %q", logged)
	}
	tracker.delivered(brb.Delivery{ID: brb.BroadcastID{Origin: 4, Sequence: 1}, Value: []byte("B")})
	sum := sha256.Sum256([]byte("B"))
	logged = captureStderr(t, func() { logDeliveries(1, tracker, time.Second) })
	if !strings.Contains(logged, "delivered=true sha256="+hex.EncodeToString(sum[:])+"\n") {
		t.Fatalf("after delivery: %q", logged)
	}
	// Without a watched broadcast the extra line is absent.
	plain := newTestTracker(time.Now)
	if logged := captureStderr(t, func() { logDeliveries(1, plain, time.Second) }); strings.Contains(logged, "watched") {
		t.Fatalf("unexpected watched line: %q", logged)
	}
}
