package bench

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/relab/gorums/benchkit"
)

// fakeSends reports fixed send timings per phase.
type fakeSends map[brb.Phase]brb.SendTiming

func (f fakeSends) SendTiming(p brb.Phase) brb.SendTiming { return f[p] }

func testSampler(t *testing.T, interval time.Duration, limits resourceLimits, res func() resources) (*sampler, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	s := newSampler(1, interval, limits, fakeSends{}, func() uint64 { return 0 }, cancel)
	s.read = func() (resources, error) { return res(), nil }
	return s, ctx
}

var sampleLine = regexp.MustCompile(`brb bench: node 1 diag sample t_ms=(\d+) `)

// sampleTimes returns the t_ms of each sample line in output, in order.
func sampleTimes(t *testing.T, output string) []int {
	t.Helper()
	var times []int
	for _, m := range sampleLine.FindAllStringSubmatch(output, -1) {
		v, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatal(err)
		}
		times = append(times, v)
	}
	return times
}

func TestSamplerLineFormat(t *testing.T) {
	start := time.Unix(100, 0)
	sends := fakeSends{
		brb.PhasePropose: {Outstanding: 1, OldestOutstanding: 20 * time.Millisecond},
		brb.PhaseEcho:    {Outstanding: 2, OldestOutstanding: 30 * time.Millisecond},
		brb.PhaseReady:   {Outstanding: 3, OldestOutstanding: 40 * time.Millisecond},
	}
	s := newSampler(7, time.Second, resourceLimits{}, sends, func() uint64 { return 9 }, func() {})
	s.read = func() (resources, error) { return resources{goroutines: 11, heapObjects: 12, heapStacks: 13}, nil }
	s.now = func() time.Time { return start.Add(1500 * time.Millisecond) }
	s.start = start
	s.completed()
	s.completed()
	got := captureStderr(t, s.sample)
	want := "brb bench: node 7 diag sample t_ms=1500 propose_out=1 echo_out=2 ready_out=3 " +
		"propose_oldest_ms=20 echo_oldest_ms=30 ready_oldest_ms=40 completions=2 deliveries=9 " +
		"goroutines=11 heap_objects=12 heap_stacks=13\n"
	if got != want {
		t.Fatalf("sample line:\n got %q\nwant %q", got, want)
	}
}

func TestSamplerInitialFinalAndJoin(t *testing.T) {
	s, _ := testSampler(t, 10*time.Millisecond, resourceLimits{}, func() resources { return resources{goroutines: 1} })
	output := captureStderr(t, func() {
		s.begin(time.Now())
		time.Sleep(55 * time.Millisecond)
		s.stop()
		time.Sleep(40 * time.Millisecond) // a sample here would follow the final one
		s.stop()                          // idempotent
	})
	times := sampleTimes(t, output)
	if len(times) < 3 || times[0] > 5 {
		t.Fatalf("want an initial sample near 0 ms, periodic samples and a final one; got t_ms %v", times)
	}
	for i := 1; i < len(times); i++ {
		if times[i] < times[i-1] {
			t.Fatalf("t_ms not monotonic: %v", times)
		}
	}
	if last := times[len(times)-1]; last < 55 || last > 90 {
		t.Fatalf("final sample at %d ms; want it at stop (about 55 ms) and none later: %v", last, times)
	}
}

func TestSamplerDisabledPrintsNothing(t *testing.T) {
	s, _ := testSampler(t, 0, resourceLimits{}, func() resources { return resources{} })
	if output := captureStderr(t, func() { s.begin(time.Now()); s.stop() }); output != "" {
		t.Fatalf("disabled sampler printed %q", output)
	}
}

func TestSamplerGuardLatchesAndCancels(t *testing.T) {
	s, ctx := testSampler(t, 5*time.Millisecond, resourceLimits{heapObjects: 500},
		func() resources { return resources{goroutines: 10, heapObjects: 1000} })
	output := captureStderr(t, func() {
		s.begin(time.Now())
		time.Sleep(30 * time.Millisecond)
		s.stop()
	})
	if s.stopReason() != "heap_objects" || ctx.Err() == nil {
		t.Fatalf("reason %q, context error %v; want heap_objects and a cancelled run", s.stopReason(), ctx.Err())
	}
	if n := strings.Count(output, "diag resource-stop"); n != 1 ||
		!strings.Contains(output, "reason=heap_objects heap_objects=1000 heap_objects_limit=500 goroutines=10 goroutines_limit=0") {
		t.Fatalf("want exactly one resource-stop line with the values; got %d in:\n%s", n, output)
	}
}

func TestSamplerGuardTripsOnFinalSample(t *testing.T) {
	// The guard trips in the final sample, which stop takes after the
	// measurement has completed; the reason is latched when stop returns, so
	// the report decision sees it.
	var over atomic.Bool
	s, ctx := testSampler(t, time.Hour, resourceLimits{goroutines: 100}, func() resources {
		if over.Load() {
			return resources{goroutines: 101}
		}
		return resources{goroutines: 50}
	})
	output := captureStderr(t, func() {
		s.begin(time.Now())
		over.Store(true)
		s.stop()
	})
	if s.stopReason() != "goroutines" || ctx.Err() == nil || strings.Count(output, "diag resource-stop") != 1 {
		t.Fatalf("reason %q, context error %v, output:\n%s", s.stopReason(), ctx.Err(), output)
	}
}

func TestSamplerNoFalseStop(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limits resourceLimits
		res    resources
	}{
		{"below limits", resourceLimits{heapObjects: 1 << 30, goroutines: 1000}, resources{goroutines: 999, heapObjects: 1 << 30}},
		{"guard off", resourceLimits{}, resources{goroutines: 1 << 40, heapObjects: 1 << 40}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := testSampler(t, 5*time.Millisecond, tc.limits, func() resources { return tc.res })
			output := captureStderr(t, func() {
				s.begin(time.Now())
				time.Sleep(20 * time.Millisecond)
				s.cancel() // normal cancellation of the run
				s.stop()
			})
			if s.stopReason() != "" || strings.Contains(output, "resource-stop") || !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("reason %q, output:\n%s", s.stopReason(), output)
			}
		})
	}
}

func TestReadResources(t *testing.T) {
	res, err := readResources()
	if err != nil {
		t.Fatal(err)
	}
	if res.goroutines == 0 || res.heapObjects == 0 || res.heapStacks == 0 {
		t.Fatalf("zero resource value: %+v", res)
	}
}

func TestDiagnosticOptions(t *testing.T) {
	base := []string{"-plaintext", "-self=127.0.0.1:9000", "-remotes=127.0.0.1:9000", "-f=0", "-output=x.binpb"}
	opts, err := ParseOptions(append(append([]string{}, base...), "-diag-interval=1s",
		"-guard-heap-objects=2147483648", "-guard-goroutines=50000"), io.Discard)
	if err != nil || opts.diagInterval != time.Second || opts.limits != (resourceLimits{heapObjects: 2147483648, goroutines: 50000}) {
		t.Fatalf("options %+v, %v", opts.limits, err)
	}
	for _, extra := range [][]string{{"-guard-goroutines=10"}, {"-guard-heap-objects=10"}, {"-diag-interval=-1s"}} {
		if _, err := ParseOptions(append(append([]string{}, base...), extra...), io.Discard); err == nil {
			t.Errorf("accepted %v", extra)
		}
	}
}

var finalCompletions = regexp.MustCompile(`diag sample t_ms=\d+ .* completions=(\d+) `)

func TestSampleCompletionsMatchTotalOps(t *testing.T) {
	peer := addresses(t, 1)[0]
	path := filepath.Join(t.TempDir(), "result.binpb")
	opts, err := ParseOptions([]string{"-plaintext", "-self=" + peer, "-remotes=" + peer, "-f=0",
		"-output=" + path, "-time=200ms", "-workers=2", "-diag-interval=20ms"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var runErr error
	output := captureStderr(t, func() { runErr = Run(ctx, opts) })
	if runErr != nil {
		t.Fatal(runErr)
	}
	matches := finalCompletions.FindAllStringSubmatch(output, -1)
	if len(matches) < 3 {
		t.Fatalf("want initial, periodic and final samples; got %d in:\n%s", len(matches), output)
	}
	report, err := benchkit.LoadReport(path)
	if err != nil {
		t.Fatal(err)
	}
	final, _ := strconv.ParseUint(matches[len(matches)-1][1], 10, 64)
	if total := report.GetResults()[0].GetTotalOps(); final != total || total == 0 {
		t.Fatalf("final sample completions=%d, report TotalOps=%d", final, total)
	}
	if strings.Contains(output, "resource-stop") {
		t.Fatal("unexpected resource stop")
	}
}

func TestResourceStopPreventsReport(t *testing.T) {
	peer := addresses(t, 1)[0]
	path := filepath.Join(t.TempDir(), "result.binpb")
	// Any running process has more than one goroutine, so the guard trips at
	// the first sample.
	opts, err := ParseOptions([]string{"-plaintext", "-self=" + peer, "-remotes=" + peer, "-f=0",
		"-output=" + path, "-time=500ms", "-workers=2", "-diag-interval=20ms", "-guard-goroutines=1"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var runErr error
	output := captureStderr(t, func() { runErr = Run(ctx, opts) })
	if runErr == nil || !strings.Contains(runErr.Error(), "resource stop (goroutines), no report") {
		t.Fatalf("Run error %v; want a resource stop", runErr)
	}
	if strings.Count(output, "diag resource-stop reason=goroutines") != 1 {
		t.Fatalf("want one resource-stop line in:\n%s", output)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("report written after a resource stop: %v", err)
	}
}

func TestSamplerMetricFailureStopsGuardedRun(t *testing.T) {
	failure := errors.New("metrics unavailable")
	for _, tc := range []struct {
		name   string
		limits resourceLimits
		stop   bool
	}{{"guarded", resourceLimits{goroutines: 100}, true}, {"unguarded", resourceLimits{}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := testSampler(t, time.Hour, tc.limits, func() resources { return resources{} })
			s.read = func() (resources, error) { return resources{}, failure }
			output := captureStderr(t, func() { s.begin(time.Now()); s.stop() })
			if !strings.Contains(output, `diag sample t_ms=0 error="metrics unavailable"`) {
				t.Fatalf("missing sample error line in:\n%s", output)
			}
			stopped := s.stopReason() == "metrics_unavailable" && ctx.Err() != nil &&
				strings.Count(output, "diag resource-stop reason=metrics_unavailable heap_objects=0 heap_objects_limit=0 goroutines=0 goroutines_limit=100") == 1
			if stopped != tc.stop || (!tc.stop && (s.stopReason() != "" || strings.Contains(output, "resource-stop"))) {
				t.Fatalf("stop %t (reason %q, context %v), want %t; output:\n%s", stopped, s.stopReason(), ctx.Err(), tc.stop, output)
			}
		})
	}
}

func TestSamplerCancelsBeforeBlockedOutput(t *testing.T) {
	s, ctx := testSampler(t, time.Hour, resourceLimits{heapObjects: 10}, func() resources { return resources{heapObjects: 11} })
	release := make(chan struct{})
	var printed atomic.Int32
	s.printf = func(string, ...any) { <-release; printed.Add(1) }
	done := make(chan struct{})
	go func() { defer close(done); s.begin(time.Now()) }()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the run was not cancelled while the diagnostic writer was blocked")
	}
	if s.stopReason() != "heap_objects" || printed.Load() != 0 {
		t.Fatalf("reason %q after %d prints; want heap_objects latched before any output", s.stopReason(), printed.Load())
	}
	close(release)
	<-done
	s.stop()
	if printed.Load() != 3 { // initial sample and its stop record, then the final sample
		t.Fatalf("%d lines printed after release, want 3", printed.Load())
	}
}

func TestRunSuppressesReportWhenFinalSampleTrips(t *testing.T) {
	peer := addresses(t, 1)[0]
	path := filepath.Join(t.TempDir(), "result.binpb")
	// With an hour-long interval, the only samples are the initial one at the
	// window start and the final one, which Run takes after the measurement has
	// succeeded and the server has stopped. Only the final one exceeds the limit.
	opts, err := ParseOptions([]string{"-plaintext", "-self=" + peer, "-remotes=" + peer, "-f=0",
		"-output=" + path, "-time=100ms", "-workers=2", "-diag-interval=1h", "-guard-goroutines=1000"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int32
	opts.readResources = func() (resources, error) {
		if reads.Add(1) == 1 {
			return resources{goroutines: 10, heapObjects: 1, heapStacks: 1}, nil
		}
		return resources{goroutines: 1001, heapObjects: 1, heapStacks: 1}, nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var runErr error
	output := captureStderr(t, func() { runErr = Run(ctx, opts) })
	if reads.Load() != 2 {
		t.Fatalf("%d metric reads, want 2 (initial and final)", reads.Load())
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "resource stop (goroutines), no report") {
		t.Fatalf("Run error %v; want a resource stop from the final sample\n%s", runErr, output)
	}
	if strings.Contains(output, "incomplete run") {
		t.Fatalf("the measurement should have succeeded before the final sample:\n%s", output)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("report written after a final-sample resource stop: %v", err)
	}
}
