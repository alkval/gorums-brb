package bench

import (
	"context"
	"fmt"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/relab/gorums/benchkit"
)

// resourceLimits is the resource guard: a sample above either limit, or a
// sample whose metrics cannot be read while a limit is set, stops the run,
// which then writes no report. Zero disables a limit. The heap limit is
// checked on samples, so it is not a hard memory limit: it excludes stacks and
// other memory, and allocation can overshoot between samples.
type resourceLimits struct {
	heapObjects uint64 // bytes of heap objects, live and not yet reclaimed
	goroutines  uint64
}

func (l resourceLimits) enabled() bool { return l.heapObjects != 0 || l.goroutines != 0 }

// resources are process-wide runtime/metrics values, read without forcing GC.
type resources struct {
	goroutines, heapObjects, heapStacks uint64
}

var resourceMetrics = [...]string{
	"/sched/goroutines:goroutines",
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/heap/stacks:bytes",
}

func readResources() (resources, error) {
	var samples [len(resourceMetrics)]metrics.Sample
	for i, name := range resourceMetrics {
		samples[i].Name = name
	}
	metrics.Read(samples[:])
	var v [len(resourceMetrics)]uint64
	for i, s := range samples {
		if s.Value.Kind() != metrics.KindUint64 {
			return resources{}, fmt.Errorf("brb bench: metric %s is unavailable", s.Name)
		}
		v[i] = s.Value.Uint64()
	}
	return resources{goroutines: v[0], heapObjects: v[1], heapStacks: v[2]}, nil
}

// sendSource is the part of a replica the sampler reads.
type sendSource interface {
	SendTiming(brb.Phase) brb.SendTiming
}

// sampler prints one diagnostic sample per interval from the start of the
// measurement window until stop, and applies the resource guard. Measurement
// only, apart from the guard, which cancels the run. Each line's fields are
// nearby snapshots, not one atomic snapshot of the replica.
type sampler struct {
	selfID      uint32
	interval    time.Duration // zero disables sampling
	limits      resourceLimits
	sends       sendSource
	deliveries  func() uint64 // cumulative local deliveries
	read        func() (resources, error)
	now         func() time.Time
	printf      func(format string, args ...any) // benchkit.Printf; a test may block it
	cancel      context.CancelFunc               // ends the run when the guard trips
	completions atomic.Uint64                    // operations recorded as successful

	startOnce, stopOnce sync.Once
	done                chan struct{} // closed when the sampling goroutine has finished
	stopping            chan struct{}
	start               time.Time

	reason     atomic.Pointer[string] // latched resource-stop reason
	net        *netCollector
	netStatus  string
	netLogOnce sync.Once
}

func newSampler(selfID uint32, interval time.Duration, limits resourceLimits, sends sendSource,
	deliveries func() uint64, cancel context.CancelFunc) *sampler {
	return &sampler{selfID: selfID, interval: interval, limits: limits, sends: sends, deliveries: deliveries,
		read: readResources, now: time.Now, printf: benchkit.Printf, cancel: cancel, stopping: make(chan struct{})}
}

// completed counts one operation where the benchmark records a success, so the
// final count equals the report's total operations.
func (s *sampler) completed() { s.completions.Add(1) }

// begin takes the initial sample at start, the window start, and then samples
// once per interval until stop. It does nothing if sampling is disabled.
func (s *sampler) begin(start time.Time) {
	if s.interval <= 0 {
		return
	}
	s.startOnce.Do(func() {
		s.start, s.done = start, make(chan struct{})
		if s.net != nil {
			s.net.begin(start)
		}
		s.sample()
		go func() {
			defer close(s.done)
			tick := time.NewTicker(s.interval)
			defer tick.Stop()
			for {
				select {
				case <-s.stopping:
					return
				case <-tick.C:
					s.sample()
				}
			}
		}()
	})
}

// stop joins the sampling goroutine and takes the final sample. Call it before
// any report decision; later calls do nothing.
func (s *sampler) stop() {
	s.stopOnce.Do(func() {
		close(s.stopping)
		if s.done == nil {
			if s.net != nil {
				s.net.stop()
				s.netStatus = s.net.shutdownStatus
			}
			return // sampling never began
		}
		<-s.done
		s.sample()
		if s.net != nil {
			s.net.stop()
			s.netStatus = s.net.shutdownStatus
		}
	})
}

// logNetStatus is called by Run's main goroutine in its shutdown defer, after
// stop (and after a successful report write). It does not rely on the worker
// being able to finish a read/write or publish a terminal marker.
func (s *sampler) logNetStatus() {
	if s.net == nil {
		return
	}
	s.stop()
	s.netLogOnce.Do(func() { s.printf("brb bench: node %d diag net-status status=%s\n", s.selfID, s.netStatus) })
}

// stopReason returns the latched resource-stop reason, or "" if the guard has
// not tripped.
func (s *sampler) stopReason() string {
	if r := s.reason.Load(); r != nil {
		return *r
	}
	return ""
}

// sample reads one sample and applies the guard before printing anything, so
// a slow diagnostic writer cannot delay a stop.
func (s *sampler) sample() {
	t := s.now().Sub(s.start)
	res, err := s.read()
	if err != nil {
		stopped := s.limits.enabled() && s.trip("metrics_unavailable")
		s.printf("brb bench: node %d diag sample t_ms=%d error=%q\n", s.selfID, t.Milliseconds(), err.Error())
		if stopped {
			s.printStop("metrics_unavailable", t, resources{})
		}
		return
	}
	reason := s.exceeded(res)
	stopped := reason != "" && s.trip(reason)
	p, e, r := s.sends.SendTiming(brb.PhasePropose), s.sends.SendTiming(brb.PhaseEcho), s.sends.SendTiming(brb.PhaseReady)
	s.printf("brb bench: node %d diag sample t_ms=%d propose_out=%d echo_out=%d ready_out=%d "+
		"propose_oldest_ms=%d echo_oldest_ms=%d ready_oldest_ms=%d completions=%d deliveries=%d "+
		"goroutines=%d heap_objects=%d heap_stacks=%d\n",
		s.selfID, t.Milliseconds(), p.Outstanding, e.Outstanding, r.Outstanding,
		p.OldestOutstanding.Milliseconds(), e.OldestOutstanding.Milliseconds(), r.OldestOutstanding.Milliseconds(),
		s.completions.Load(), s.deliveries(), res.goroutines, res.heapObjects, res.heapStacks)
	if stopped {
		s.printStop(reason, t, res)
	}
}

// exceeded returns the limit res exceeds, or "".
func (s *sampler) exceeded(res resources) string {
	switch {
	case s.limits.heapObjects != 0 && res.heapObjects > s.limits.heapObjects:
		return "heap_objects"
	case s.limits.goroutines != 0 && res.goroutines > s.limits.goroutines:
		return "goroutines"
	}
	return ""
}

// trip latches reason and cancels the run. It reports whether this call
// latched; only the first reason is kept.
func (s *sampler) trip(reason string) bool {
	if !s.reason.CompareAndSwap(nil, &reason) {
		return false
	}
	s.cancel()
	return true
}

// printStop prints the resource-stop record after the run was cancelled. The
// values are zero when the metrics could not be read.
func (s *sampler) printStop(reason string, t time.Duration, res resources) {
	s.printf("brb bench: node %d diag resource-stop reason=%s heap_objects=%d heap_objects_limit=%d "+
		"goroutines=%d goroutines_limit=%d t_ms=%d\n", s.selfID, reason, res.heapObjects, s.limits.heapObjects,
		res.goroutines, s.limits.goroutines, t.Milliseconds())
}
