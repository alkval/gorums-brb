package bench

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/relab/gorums/benchkit"
)

// captureStderr runs fn while collecting what benchkit.Printf writes. Stderr is
// restored even if fn stops the test.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	os.Stderr = w
	var once sync.Once
	restore := func() { once.Do(func() { os.Stderr = stderr; _ = w.Close() }) }
	defer restore()
	output := make(chan string, 1)
	go func() {
		defer r.Close()
		var b strings.Builder
		_, _ = io.Copy(&b, r)
		output <- b.String()
	}()
	fn()
	restore()
	return <-output
}

// newTestTracker returns a tracker for replica 1 that reads its clock from now.
func newTestTracker(now func() time.Time) *deliveryTracker {
	return &deliveryTracker{self: 1, now: now, waiters: make(map[uint64]chan time.Time)}
}

// Deliveries from every origin count toward the total, and those recorded in
// the window [start, end) also count toward in_window. The window is empty
// until it is opened.
func TestDeliveryTrackerWindow(t *testing.T) {
	base := time.Now()
	clock := base
	tracker := newTestTracker(func() time.Time { return clock })
	var wantIn, wantTotal uint64
	deliver := func(name string, origin brb.ProcessID, at time.Duration, inWindow bool) {
		t.Helper()
		clock = base.Add(at)
		tracker.delivered(brb.Delivery{ID: brb.BroadcastID{Origin: origin, Sequence: wantTotal + 1}})
		wantTotal++
		if inWindow {
			wantIn++
		}
		if in, total := tracker.counts(); in != wantIn || total != wantTotal {
			t.Fatalf("%s: in_window=%d total=%d, want %d and %d", name, in, total, wantIn, wantTotal)
		}
	}
	deliver("unopened window", 2, 0, false)
	deliver("unopened window, later time", 3, 20*time.Millisecond, false)

	clock = base.Add(10 * time.Millisecond)
	start, end := tracker.open(100 * time.Millisecond)
	if !start.Equal(clock) || end.Sub(start) != 100*time.Millisecond {
		t.Fatalf("open returned [%v, %v), want [%v, +100ms)", start, end, clock)
	}
	deliver("before start", 2, 5*time.Millisecond, false)
	deliver("at start", 1, 10*time.Millisecond, true)
	for origin := brb.ProcessID(1); origin <= 4; origin++ {
		deliver(fmt.Sprintf("inside, origin %d", origin), origin, 50*time.Millisecond, true)
	}
	deliver("just before end", 3, 110*time.Millisecond-time.Nanosecond, true)
	deliver("at end", 4, 110*time.Millisecond, false)
	deliver("after end", 2, time.Second, false)
}

// A replica's own deliveries also wake the broadcast waiting for them, with the
// time the delivery was recorded.
func TestDeliveryTrackerWakesOwnBroadcast(t *testing.T) {
	at := time.Now()
	tracker := newTestTracker(func() time.Time { return at })
	own, other := make(chan time.Time, 1), make(chan time.Time, 1)
	tracker.waiters[7] = own
	tracker.waiters[8] = other
	tracker.delivered(brb.Delivery{ID: brb.BroadcastID{Origin: 2, Sequence: 8}})
	tracker.delivered(brb.Delivery{ID: brb.BroadcastID{Origin: 1, Sequence: 7}})
	if got := <-own; !got.Equal(at) {
		t.Fatalf("waiter got %v, want %v", got, at)
	}
	if len(other) != 0 || tracker.waiters[8] == nil {
		t.Fatal("a delivery from another origin woke this replica's broadcast")
	}
}

// Opening the window while deliveries run classifies each delivery by the
// clock reading it took under the tracker's lock: those before the window's
// start were recorded before it was opened, and all later ones are inside it.
func TestDeliveryTrackerConcurrentOpen(t *testing.T) {
	const workers, perWorker = 8, 2000
	base := time.Now()
	var tick atomic.Int64
	tracker := newTestTracker(func() time.Time { return base.Add(time.Duration(tick.Add(1))) })
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				tracker.delivered(brb.Delivery{ID: brb.BroadcastID{Origin: brb.ProcessID(w%4 + 2), Sequence: uint64(i + 1)}})
			}
		})
	}
	for tick.Load() < workers*perWorker/2 {
		runtime.Gosched()
	}
	start, _ := tracker.open(time.Hour)
	wg.Wait()
	// Every clock reading is a distinct tick: one per delivery and one for open.
	// The readings after start's tick all come from deliveries inside the window.
	ticks := uint64(workers*perWorker + 1)
	wantIn := ticks - uint64(start.Sub(base))
	if in, total := tracker.counts(); in != wantIn || total != workers*perWorker {
		t.Fatalf("in_window=%d total=%d, want %d and %d", in, total, wantIn, workers*perWorker)
	}
}

// The logged counts are a snapshot. A delivery whose callback runs after the
// report is written is missing from it.
func TestLogDeliveriesIsASnapshot(t *testing.T) {
	base := time.Now()
	clock := base
	tracker := newTestTracker(func() time.Time { return clock })
	tracker.open(time.Second)
	for seq := range uint64(2) {
		tracker.delivered(brb.Delivery{ID: brb.BroadcastID{Origin: 2, Sequence: seq + 1}})
	}
	logged := captureStderr(t, func() { logDeliveries(1, tracker, time.Second) })
	clock = base.Add(2 * time.Second)
	tracker.delivered(brb.Delivery{ID: brb.BroadcastID{Origin: 3, Sequence: 1}})

	want := "brb bench: node 1 deliveries window_ns=1000000000 in_window=2 total=2\n"
	if logged != want {
		t.Fatalf("logged %q, want %q", logged, want)
	}
	if in, total := tracker.counts(); in != 2 || total != 3 {
		t.Fatalf("after the late delivery: in_window=%d total=%d, want 2 and 3", in, total)
	}
}

// Each replica logs its deliveries from every origin. Deliveries still in a
// handler at shutdown may be missing, so the total is at most the number of
// broadcasts that origins completed, and more than any single origin's share.
func TestBenchmarkLogsReplicaDeliveries(t *testing.T) {
	peers := addresses(t, 4)
	paths := make([]string, 4)
	errs := make([]error, 4)
	logged := captureStderr(t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		done := make(chan struct{}, 4)
		for i, peer := range peers {
			paths[i] = filepath.Join(t.TempDir(), "result.binpb")
			opts, err := ParseOptions([]string{"-plaintext", "-self=" + peer, "-remotes=" + strings.Join(peers, ","),
				"-f=1", "-output=" + paths[i], "-time=200ms", "-payload=64"}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			go func() { errs[i] = Run(ctx, opts); done <- struct{}{} }()
		}
		for range 4 {
			<-done
		}
	})
	var completed, largest uint64
	opsByNode := make(map[int]uint64)
	for i, path := range paths {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		report, err := benchkit.LoadReport(path)
		if err != nil {
			t.Fatal(err)
		}
		ops := report.GetResults()[0].GetTotalOps()
		opsByNode[i+1] = ops
		completed += ops
		largest = max(largest, ops)
	}
	for id := 1; id <= 4; id++ {
		line := fmt.Sprintf("brb bench: node %d deliveries ", id)
		i := strings.Index(logged, line)
		if i < 0 {
			t.Fatalf("missing deliveries line for node %d in:\n%s", id, logged)
		}
		var window int64
		var inWindow, total uint64
		if _, err := fmt.Sscanf(logged[i+len(line):], "window_ns=%d in_window=%d total=%d", &window, &inWindow, &total); err != nil {
			t.Fatal(err)
		}
		if window != (200*time.Millisecond).Nanoseconds() || inWindow == 0 || inWindow > total || total > completed || total <= largest {
			t.Fatalf("node %d: window=%d in_window=%d total=%d, want 200ms and 0 < in_window <= total <= %d, total > %d",
				id, window, inWindow, total, completed, largest)
		}
		// Every recorded latency is one delivery the worker observed after its
		// Broadcast returned or before; none is still running after a full run.
		if o := checkDiagnostics(t, logged, id); o[3] != 0 || o[4]+o[5] != opsByNode[id] {
			t.Fatalf("node %d: diag origin %v, want no outstanding and before+after = TotalOps %d", id, o, opsByNode[id])
		}
		if !strings.Contains(logged, fmt.Sprintf("brb bench: node %d diag window violations=0\n", id)) ||
			strings.Contains(logged, fmt.Sprintf("brb bench: node %d diag window first", id)) {
			t.Fatalf("node %d: unexpected window violation in a fault-free run:\n%s", id, logged)
		}
	}
}
