package bench

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/relab/gorums"
	"github.com/relab/gorums/benchkit"
)

// originTiming records when each origin broadcast's Broadcast call returned
// relative to its local delivery. Time spent in Broadcast after delivery
// lowers throughput without raising the recorded latency. Measurement only.
type originTiming struct {
	mu                          sync.Mutex
	started, returned, failed   uint64
	beforeDelivery, undelivered uint64
	afterDelivery               []time.Duration // Broadcast return minus delivery
}

func (o *originTiming) start() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.started++
}

// finish records one Broadcast call that returned at returnedAt. delivered is
// the delivery time, or the zero time if the worker saw no delivery.
func (o *originTiming) finish(returnedAt, deliveredAt time.Time, failed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.returned++
	if failed {
		o.failed++
	}
	switch {
	case deliveredAt.IsZero():
		o.undelivered++
	case returnedAt.After(deliveredAt):
		o.afterDelivery = append(o.afterDelivery, returnedAt.Sub(deliveredAt))
	default:
		o.beforeDelivery++
	}
}

var errServerStopped = errors.New("brb bench: server stopped")

// settleBroadcast records the origin timing of a Broadcast call that returned
// at returnedAt with err and, unless err ends the attempt, waits for the local
// delivery. A send failure to some peer does not end it. A delivery already
// buffered when the attempt ends is still recorded; the error is returned
// unchanged and the broadcast does not count as completed.
func settleBroadcast(ctx context.Context, origin *originTiming, ch <-chan time.Time, serveErr <-chan error,
	returnedAt time.Time, err error) (time.Time, error) {
	if err != nil && !errors.Is(err, gorums.ErrSendFailure) {
		origin.finish(returnedAt, bufferedDelivery(ch), true)
		return time.Time{}, err
	}
	deliveredAt, waitErr := awaitDelivery(ctx, ch, serveErr)
	origin.finish(returnedAt, deliveredAt, err != nil)
	return deliveredAt, waitErr
}

// awaitDelivery waits for the local delivery of an origin broadcast. If the run
// ends or the server stops first, it still returns a delivery time that is
// already buffered, so the origin timing keeps that evidence; the error is
// returned unchanged and the broadcast does not count as completed.
func awaitDelivery(ctx context.Context, ch <-chan time.Time, serveErr <-chan error) (time.Time, error) {
	select {
	case deliveredAt := <-ch:
		return deliveredAt, nil
	case <-ctx.Done():
		return bufferedDelivery(ch), ctx.Err()
	case err := <-serveErr:
		return bufferedDelivery(ch), fmt.Errorf("%w: %w", errServerStopped, err)
	}
}

// bufferedDelivery returns a delivery time already waiting in ch, or the zero
// time if there is none. It does not block.
func bufferedDelivery(ch <-chan time.Time) time.Time {
	select {
	case deliveredAt := <-ch:
		return deliveredAt
	default:
		return time.Time{}
	}
}

// originSummary is a consistent snapshot of originTiming. Outstanding calls had
// not returned when it was taken (censored).
type originSummary struct {
	Started, Returned, Failed, Outstanding     uint64
	BeforeDelivery, AfterDelivery, Undelivered uint64
	P50, P99, Max                              time.Duration
	Over                                       [len(brb.SendThresholds)]uint64
}

func (o *originTiming) summary() originSummary {
	o.mu.Lock()
	delays := slices.Clone(o.afterDelivery)
	s := originSummary{Started: o.started, Returned: o.returned, Failed: o.failed,
		Outstanding: o.started - o.returned, BeforeDelivery: o.beforeDelivery, Undelivered: o.undelivered}
	o.mu.Unlock()
	s.AfterDelivery = uint64(len(delays))
	if len(delays) == 0 {
		return s
	}
	slices.Sort(delays)
	s.P50, s.P99, s.Max = delays[len(delays)/2], delays[len(delays)*99/100], delays[len(delays)-1]
	for _, d := range delays {
		for i, bound := range brb.SendThresholds {
			if d > bound {
				s.Over[i]++
			}
		}
	}
	return s
}

// logDiagnostics prints the diagnostic summary once per replica, after the run
// is cancelled and the server stopped. It is also called for invalid runs, which
// write no report. Calls still in progress are reported as outstanding.
func logDiagnostics(selfID uint32, replica *brb.Replica, origin *originTiming) {
	o := origin.summary()
	benchkit.Printf("brb bench: node %d diag origin started=%d returned=%d failed=%d outstanding=%d "+
		"before_delivery=%d after_delivery=%d undelivered=%d after_p50_ns=%d after_p99_ns=%d after_max_ns=%d "+
		"after_over10ms=%d after_over100ms=%d after_over1s=%d\n",
		selfID, o.Started, o.Returned, o.Failed, o.Outstanding, o.BeforeDelivery, o.AfterDelivery, o.Undelivered,
		o.P50.Nanoseconds(), o.P99.Nanoseconds(), o.Max.Nanoseconds(), o.Over[0], o.Over[1], o.Over[2])
	for _, p := range []struct {
		name  string
		phase brb.Phase
	}{{"propose", brb.PhasePropose}, {"echo", brb.PhaseEcho}, {"ready", brb.PhaseReady}, {"qecho", brb.PhaseQEcho}} {
		t := replica.SendTiming(p.phase)
		benchkit.Printf("brb bench: node %d diag send phase=%s started=%d succeeded=%d failed=%d cancelled=%d "+
			"outstanding=%d total_ns=%d max_ns=%d over10ms=%d over100ms=%d over1s=%d oldest_outstanding_ns=%d\n",
			selfID, p.name, t.Started, t.Succeeded, t.Failed, t.Cancelled, t.Outstanding, t.Total.Nanoseconds(),
			t.Max.Nanoseconds(), t.Over[0], t.Over[1], t.Over[2], t.OldestOutstanding.Nanoseconds())
	}
	logQueues(selfID, replica)
	count, w := replica.WindowViolations()
	benchkit.Printf("brb bench: node %d diag window violations=%d\n", selfID, count)
	if w == nil {
		return
	}
	payloads := "not-retained"
	if w.Payloads != nil {
		payloads = joinFingerprints(w.Payloads)
	}
	benchkit.Printf("brb bench: node %d diag window first origin=%d phase=%s sender=%d sequence=%d prefix=%d open=%d "+
		"next_allocated=%t later_delivered=%d highest_delivered=%d lowest=%d lowest_allocated=%t started=%t "+
		"echo_sent=%t ready_sent=%t proposed=%s echoes=%s readies=%s payloads=%s\n",
		selfID, w.Origin, phaseName(w.Phase), w.Sender, w.Sequence, w.Prefix, w.Open, w.NextAllocated, w.LaterDelivered,
		w.HighestDelivered, w.Lowest, w.LowestAllocated, w.Started, w.EchoSent, w.ReadySent, proposed(w),
		joinVotes(w.Echoes), joinVotes(w.Readies), payloads)
}

// logQueues prints the dispatch queue limits and high-water marks, the local
// delivery queue, failed sends and, for an invalid run, the first capacity
// failure. Call it after the replica has stopped.
func logQueues(selfID uint32, replica *brb.Replica) {
	self := replica.SelfQueue()
	peers := replica.PeerQueues()
	var peerMax brb.QueueSnapshot
	if len(peers) != 0 {
		peerMax = peers[0].Queue
	}
	benchkit.Printf("brb bench: node %d diag queue-limits peer_max_entries=%d peer_max_bytes=%d self_max_entries=%d self_max_bytes=%d\n",
		selfID, peerMax.MaxEntries, peerMax.MaxBytes, self.Queue.MaxEntries, self.Queue.MaxBytes)
	for _, p := range peers {
		q := p.Queue
		benchkit.Printf("brb bench: node %d diag peer-queue peer=%d high_entries=%d high_bytes=%d queued=%d in_flight=%d "+
			"discarded_at_stop=%d discarded_bytes=%d\n",
			selfID, p.Peer, q.HighWaterEntries, q.HighWaterBytes, q.QueuedEntries, q.InFlightEntries,
			p.DiscardedAtStopEntries, p.DiscardedAtStopBytes)
	}
	benchkit.Printf("brb bench: node %d diag self-queue high_entries=%d high_bytes=%d queued=%d in_flight=%d max_callback_lag_ns=%d\n",
		selfID, self.Queue.HighWaterEntries, self.Queue.HighWaterBytes, self.Queue.QueuedEntries, self.Queue.InFlightEntries,
		self.MaxCallbackLag.Nanoseconds())
	benchkit.Printf("brb bench: node %d diag send-failures propose=%d echo=%d ready=%d qecho=%d\n", selfID,
		replica.SendFailures(brb.PhasePropose), replica.SendFailures(brb.PhaseEcho),
		replica.SendFailures(brb.PhaseReady), replica.SendFailures(brb.PhaseQEcho))
	if c := replica.CapacityStatus(); c != nil {
		benchkit.Printf("brb bench: node %d diag capacity peer=%d phase=%s origin=%d sequence=%d msg_bytes=%d queued=%d in_flight=%d "+
			"queued_bytes=%d in_flight_bytes=%d max_entries=%d max_bytes=%d latched_unix_ns=%d\n",
			selfID, c.Peer, phaseName(c.Phase), c.ID.Origin, c.ID.Sequence, c.MsgBytes, c.Queue.QueuedEntries,
			c.Queue.InFlightEntries, c.Queue.QueuedBytes, c.Queue.InFlightBytes, c.Queue.MaxEntries, c.Queue.MaxBytes,
			c.At.UnixNano())
	}
}

func phaseName(p brb.Phase) string {
	switch p {
	case brb.PhasePropose:
		return "propose"
	case brb.PhaseEcho:
		return "echo"
	case brb.PhaseReady:
		return "ready"
	case brb.PhaseQEcho:
		return "qecho"
	}
	return "unknown"
}

// proposed reports PROPOSE receipt, which only the READY-digest variant keeps.
func proposed(w *brb.WindowSnapshot) string {
	switch {
	case w.Payloads == nil:
		return "not-retained"
	case w.Proposed:
		return "true"
	}
	return "false"
}

func joinVotes(votes []brb.VoteCount) string {
	if len(votes) == 0 {
		return "none"
	}
	parts := make([]string, len(votes))
	for i, v := range votes {
		parts[i] = fmt.Sprintf("%s:%d", v.Fingerprint, v.Count)
	}
	return strings.Join(parts, ",")
}

func joinFingerprints(fps []string) string {
	if len(fps) == 0 {
		return "none"
	}
	return strings.Join(fps, ",")
}
