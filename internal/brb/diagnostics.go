package brb

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"
)

// Diagnostic measurement only: nothing here changes thresholds, messages,
// state transitions, the window or send behaviour.

// SendThresholds are the duration bounds counted by SendTiming.Over.
var SendThresholds = [3]time.Duration{10 * time.Millisecond, 100 * time.Millisecond, time.Second}

// SendTiming summarises the Gorums Send calls of one phase. Durations cover
// completed calls only. Calls still running when the snapshot is taken are
// Outstanding; their durations are censored, and OldestOutstanding is the age
// of the longest-running one at that time.
type SendTiming struct {
	Started, Succeeded, Failed, Cancelled, Outstanding uint64
	Total, Max, OldestOutstanding                      time.Duration
	Over                                               [len(SendThresholds)]uint64
}

// sendStats records Send calls without holding its lock across a call.
type sendStats struct {
	mu       sync.Mutex
	next     uint64
	inflight map[uint64]time.Time
	timing   SendTiming
}

func (s *sendStats) begin() (token uint64, start time.Time) {
	start = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight == nil {
		s.inflight = make(map[uint64]time.Time)
	}
	s.next++
	s.inflight[s.next] = start
	s.timing.Started++
	return s.next, start
}

// end records a finished call. A failure counts as cancelled when it reports
// context cancellation or the run had already ended; other failures happened
// during the active run.
func (s *sendStats) end(token uint64, start time.Time, err error, runEnded bool) {
	d := time.Since(start)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, token)
	switch {
	case err == nil:
		s.timing.Succeeded++
	case errors.Is(err, context.Canceled) || runEnded:
		s.timing.Cancelled++
	default:
		s.timing.Failed++
	}
	s.timing.Total += d
	s.timing.Max = max(s.timing.Max, d)
	for i, bound := range SendThresholds {
		if d > bound {
			s.timing.Over[i]++
		}
	}
}

func (s *sendStats) snapshot() SendTiming {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Read the clock under the lock, so the ages match the counts.
	now := time.Now()
	t := s.timing
	t.Outstanding = uint64(len(s.inflight))
	for _, start := range s.inflight {
		t.OldestOutstanding = max(t.OldestOutstanding, now.Sub(start))
	}
	return t
}

// VoteCount is the number of distinct senders that voted for one value,
// identified by a fingerprint: the first 8 bytes of the value's SHA-256 digest
// in hex. Bracha votes are keyed by value and READY-digest votes by digest, so
// the same value has the same fingerprint in both.
type VoteCount struct {
	Fingerprint string
	Count       int
}

// WindowSnapshot describes a replica's state for one origin at its first
// receive-window violation. It is captured in the critical section that
// rejects the message and holds copies only, never payloads.
type WindowSnapshot struct {
	Sender, Origin ProcessID
	Phase          Phase
	Sequence       uint64 // the rejected sequence
	Prefix         uint64 // the origin's contiguous delivered prefix
	Open           int    // retained instances of the origin within the window
	NextAllocated  bool   // whether Prefix+1 has an instance
	LaterDelivered int    // delivered instances beyond the prefix
	// Highest delivered sequence beyond the prefix, or 0 if none.
	HighestDelivered uint64
	// The lowest undelivered sequence above the prefix and, if it has an
	// instance, its state. Payloads is nil outside the READY-digest variant,
	// which is the only variant that retains values apart from vote keys.
	Lowest                       uint64
	LowestAllocated              bool
	Started, EchoSent, ReadySent bool
	Proposed                     bool // READY-digest variant only
	Echoes, Readies              []VoteCount
	Payloads                     []string
}

// fingerprint identifies a vote key: a value, or in the READY-digest variant a
// digest.
func (m *StateMachine) fingerprint(key string) string {
	if m.readyDigest {
		return hex.EncodeToString([]byte(key)[:min(8, len(key))])
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:8])
}

func (m *StateMachine) voteCounts(v *votes) []VoteCount {
	counts := make([]VoteCount, 0, len(v.counts))
	for key, count := range v.counts {
		counts = append(counts, VoteCount{m.fingerprint(key), count})
	}
	slices.SortFunc(counts, func(a, b VoteCount) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), strings.Compare(a.Fingerprint, b.Fingerprint))
	})
	return counts
}

// recordWindowViolation counts a rejected message and captures the snapshot on
// the first one. The caller holds m.mu.
func (m *StateMachine) recordWindowViolation(from ProcessID, phase Phase, id BroadcastID) {
	m.windowViolations++
	if m.firstViolation != nil {
		return
	}
	prefix := m.delivered[id.Origin]
	s := &WindowSnapshot{Sender: from, Origin: id.Origin, Phase: phase, Sequence: id.Sequence, Prefix: prefix}
	for seq := prefix + 1; seq <= prefix+WindowSize && seq > prefix; seq++ {
		state := m.instances[BroadcastID{id.Origin, seq}]
		if seq == prefix+1 {
			s.NextAllocated = state != nil
		}
		if state == nil {
			if s.Lowest == 0 {
				s.Lowest = seq
			}
			continue
		}
		s.Open++
		if state.delivered {
			s.LaterDelivered++
			s.HighestDelivered = seq
			continue
		}
		if s.Lowest == 0 {
			s.Lowest, s.LowestAllocated = seq, true
			s.Started, s.EchoSent, s.ReadySent, s.Proposed = state.started, state.echoSent, state.readySent, state.proposed
			s.Echoes, s.Readies = m.voteCounts(&state.echoes), m.voteCounts(&state.readies)
			if m.readyDigest {
				s.Payloads = make([]string, 0, len(state.payloads))
				for digest := range state.payloads {
					s.Payloads = append(s.Payloads, m.fingerprint(digest))
				}
				slices.Sort(s.Payloads)
			}
		}
	}
	m.firstViolation = s
}

// noteEcho counts an accepted ECHO vote that is the first for its value in an
// instance that already holds votes for another value. The caller holds m.mu.
func (m *StateMachine) noteEcho(v *votes, count int) {
	if count == 1 && len(v.counts) > 1 {
		m.conflictingEchoes++
	}
}

// ConflictingEchoes reports how many accepted ECHO votes introduced a further
// distinct value in their instance: one per extra value. With a correct origin
// and correct senders it stays zero. A nonzero count is evidence of conflicting
// ECHO values, not of which sender is faulty: after an origin equivocates, a
// correct sender's ECHO can add the second value. Votes arriving after an
// instance delivered are not counted, so it is a lower bound and can be zero
// in a run with a faulty sender.
func (m *StateMachine) ConflictingEchoes() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conflictingEchoes
}

// WindowViolations returns the number of messages rejected for being outside
// the receive window and, if there was one, a copy of the first snapshot.
func (m *StateMachine) WindowViolations() (uint64, *WindowSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.firstViolation == nil {
		return m.windowViolations, nil
	}
	s := *m.firstViolation
	s.Echoes, s.Readies, s.Payloads = slices.Clone(s.Echoes), slices.Clone(s.Readies), slices.Clone(s.Payloads)
	return m.windowViolations, &s
}

// QueueSnapshot is the logical occupancy of one bounded dispatch queue.
// Entries and bytes count queued items plus the item a worker has taken but
// not finished (in flight). Bytes are marshalled message sizes, or value
// sizes for deliveries, plus a fixed per-entry charge. High-water marks cover
// the replica's lifetime.
type QueueSnapshot struct {
	QueuedEntries, InFlightEntries int
	QueuedBytes, InFlightBytes     int64
	MaxEntries                     int
	MaxBytes                       int64
	HighWaterEntries               int
	HighWaterBytes                 int64
}

func queueSnapshot(c credits) QueueSnapshot {
	return QueueSnapshot{
		QueuedEntries: c.queuedEntries, InFlightEntries: c.inFlightEntries,
		QueuedBytes: c.queuedBytes, InFlightBytes: c.inFlightBytes,
		MaxEntries: c.maxEntries, MaxBytes: c.maxBytes,
		HighWaterEntries: c.highEntries, HighWaterBytes: c.highBytes,
	}
}

// PeerQueueSnapshot describes the outbound queue to one remote peer. Items
// still queued when the replica stopped were discarded and are counted here.
type PeerQueueSnapshot struct {
	Peer                   ProcessID
	Queue                  QueueSnapshot
	DiscardedAtStopEntries int
	DiscardedAtStopBytes   int64
}

// SelfQueueSnapshot describes the local delivery queue. MaxCallbackLag is the
// longest time from a delivery's transition to the start of its callback.
type SelfQueueSnapshot struct {
	Queue          QueueSnapshot
	MaxCallbackLag time.Duration
}

// CapacitySnapshot records the first queue that could not accept required
// work. Peer is this replica's own ID for the local delivery queue. At is when
// the latch tripped, on this host's clock.
type CapacitySnapshot struct {
	Peer     ProcessID
	Phase    Phase
	ID       BroadcastID
	MsgBytes int64
	Queue    QueueSnapshot
	At       time.Time
}

// SendFailureSnapshot records the first failed send to a peer while the run
// was active.
type SendFailureSnapshot struct {
	Peer  ProcessID
	Phase Phase
	ID    BroadcastID
	Err   string
}
