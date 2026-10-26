package brb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSendStatsClassifiesCalls(t *testing.T) {
	var s sendStats
	finish := func(err error, runEnded bool, took time.Duration) {
		token, _ := s.begin()
		s.end(token, time.Now().Add(-took), err, runEnded)
	}
	finish(nil, false, 0)
	finish(nil, false, 150*time.Millisecond)
	finish(errors.New("send failure"), false, 20*time.Millisecond)
	finish(fmt.Errorf("wrapped: %w", context.Canceled), false, 0)
	finish(errors.New("send failure"), true, 2*time.Second)
	token, _ := s.begin() // still running when the snapshot is taken
	defer s.end(token, time.Now(), nil, false)
	time.Sleep(5 * time.Millisecond)

	got := s.snapshot()
	if got.Started != 6 || got.Succeeded != 2 || got.Failed != 1 || got.Cancelled != 2 || got.Outstanding != 1 {
		t.Fatalf("counts %+v, want started=6 succeeded=2 failed=1 cancelled=2 outstanding=1", got)
	}
	// Completed calls only: 0, 150 ms, 20 ms, 0 and 2 s.
	if got.Over != [3]uint64{3, 2, 1} {
		t.Fatalf("over thresholds %v, want [3 2 1]", got.Over)
	}
	if got.Max < 2*time.Second || got.Total < 2170*time.Millisecond {
		t.Fatalf("max %v total %v, want >= 2s and >= 2.17s", got.Max, got.Total)
	}
	if got.OldestOutstanding < 5*time.Millisecond {
		t.Fatalf("oldest outstanding %v, want the censored call's age", got.OldestOutstanding)
	}
}

func TestSendStatsConcurrent(t *testing.T) {
	var s sendStats
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				token, start := s.begin()
				s.end(token, start, nil, false)
			}
		})
	}
	wg.Wait()
	if got := s.snapshot(); got.Started != 8000 || got.Succeeded != 8000 || got.Outstanding != 0 {
		t.Fatalf("counts %+v, want 8000 started and succeeded, none outstanding", got)
	}
}

func fp(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

func mustHandle(t *testing.T, m *StateMachine, from ProcessID, phase Phase, seq uint64, value string) []Action {
	t.Helper()
	actions, err := m.Handle(from, phase, BroadcastID{1, seq}, []byte(value))
	if err != nil {
		t.Fatalf("seq %d: %v", seq, err)
	}
	return actions
}

func reject(t *testing.T, m *StateMachine, from ProcessID, phase Phase, seq uint64) {
	t.Helper()
	if _, err := m.Handle(from, phase, BroadcastID{1, seq}, []byte("late")); !errors.Is(err, ErrOutsideWindow) {
		t.Fatalf("seq %d: got %v, want ErrOutsideWindow", seq, err)
	}
}

// A delivered later instance with nothing at prefix+1 is a prefix hole. The
// window admits prefix+1024 and rejects prefix+1025.
func TestWindowSnapshotPrefixHole(t *testing.T) {
	m, err := NewStateMachine(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	for from := ProcessID(1); from <= 3; from++ {
		mustHandle(t, m, from, PhaseReady, 2, "v")
	}
	if count, s := m.WindowViolations(); count != 0 || s != nil {
		t.Fatalf("violations before any: %d %v", count, s)
	}
	mustHandle(t, m, 2, PhaseEcho, WindowSize, "edge")
	reject(t, m, 3, PhaseEcho, WindowSize+1)

	count, s := m.WindowViolations()
	want := WindowSnapshot{Sender: 3, Origin: 1, Phase: PhaseEcho, Sequence: WindowSize + 1, Prefix: 0, Open: 2,
		NextAllocated: false, LaterDelivered: 1, HighestDelivered: 2, Lowest: 1, LowestAllocated: false}
	if count != 1 || s == nil || fmt.Sprint(*s) != fmt.Sprint(want) {
		t.Fatalf("got %d %+v, want 1 %+v", count, s, want)
	}
}

// An allocated but undelivered prefix+1 records its votes, each with the
// fingerprint of the value it counts.
func TestWindowSnapshotUndeliveredPrefix(t *testing.T) {
	m, err := NewStateMachine(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, m, 1, PhasePropose, 1, "v")
	mustHandle(t, m, 2, PhaseEcho, 1, "v")
	mustHandle(t, m, 3, PhaseEcho, 1, "v")
	mustHandle(t, m, 4, PhaseEcho, 1, "w")
	mustHandle(t, m, 4, PhaseReady, 1, "v")
	reject(t, m, 2, PhaseReady, 5000)

	_, s := m.WindowViolations()
	if s == nil || !s.NextAllocated || s.Lowest != 1 || !s.LowestAllocated || !s.EchoSent || s.ReadySent || s.Started {
		t.Fatalf("snapshot %+v", s)
	}
	if fmt.Sprint(s.Echoes) != fmt.Sprint([]VoteCount{{fp("v"), 2}, {fp("w"), 1}}) ||
		fmt.Sprint(s.Readies) != fmt.Sprint([]VoteCount{{fp("v"), 1}}) {
		t.Fatalf("votes echoes=%v readies=%v", s.Echoes, s.Readies)
	}
	if s.Payloads != nil {
		t.Fatalf("Bracha retains no payloads, got %v", s.Payloads)
	}
}

// In the READY-digest variant, a READY quorum without a value shows as no
// stored payload, and READY and ECHO fingerprints agree for the same value.
func TestWindowSnapshotDigestWithoutValue(t *testing.T) {
	m, err := NewReadyDigestStateMachine(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("v"))
	for from := ProcessID(2); from <= 4; from++ {
		if _, err := m.HandleReadyDigest(from, BroadcastID{1, 1}, digest[:]); err != nil {
			t.Fatal(err)
		}
	}
	mustHandle(t, m, 2, PhaseEcho, 2, "v")
	if _, err := m.HandleReadyDigest(3, BroadcastID{1, 2}, digest[:]); err != nil {
		t.Fatal(err)
	}
	reject(t, m, 4, PhaseEcho, 3000)

	_, s := m.WindowViolations()
	if s == nil || s.Lowest != 1 || !s.LowestAllocated || s.Proposed || !s.ReadySent || s.Payloads == nil || len(s.Payloads) != 0 {
		t.Fatalf("snapshot %+v", s)
	}
	if fmt.Sprint(s.Readies) != fmt.Sprint([]VoteCount{{fp("v"), 3}}) || len(s.Echoes) != 0 {
		t.Fatalf("votes echoes=%v readies=%v", s.Echoes, s.Readies)
	}
	if s.Open != 2 || s.LaterDelivered != 0 {
		t.Fatalf("open %d later delivered %d", s.Open, s.LaterDelivered)
	}
}

// Only the first violation is captured; every rejected message is counted,
// duplicates included. The returned snapshot is a copy.
func TestWindowSnapshotFirstOnlyAndCopied(t *testing.T) {
	m, err := NewStateMachine(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, m, 2, PhaseEcho, 1, "v")
	reject(t, m, 2, PhaseEcho, 2000)
	reject(t, m, 2, PhaseEcho, 2000)
	reject(t, m, 3, PhaseReady, 3000)
	count, first := m.WindowViolations()
	if count != 3 || first.Sequence != 2000 || first.Sender != 2 {
		t.Fatalf("got %d %+v, want 3 violations and the first snapshot", count, first)
	}
	first.Echoes[0].Count = 99
	// Later progress does not change the captured snapshot.
	for from := ProcessID(1); from <= 3; from++ {
		mustHandle(t, m, from, PhaseReady, 1, "v")
	}
	_, again := m.WindowViolations()
	if again.Echoes[0].Count != 1 || again.Prefix != 0 || again.Lowest != 1 {
		t.Fatalf("snapshot changed: %+v", again)
	}
	// Handling continues normally after a violation.
	if count, _ := m.WindowViolations(); count != 3 || m.delivered[1] != 1 {
		t.Fatalf("count %d prefix %d", count, m.delivered[1])
	}
}

// BenchmarkSendStats measures the bookkeeping added to each Send call: one
// begin and one end, including their clock reads. Run it with
//
//	go test -run '^$' -bench SendStats ./internal/brb
func BenchmarkSendStats(b *testing.B) {
	var s sendStats
	b.ReportAllocs()
	for b.Loop() {
		token, start := s.begin()
		s.end(token, start, nil, false)
	}
}

func BenchmarkSendStatsParallel(b *testing.B) {
	var s sendStats
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			token, start := s.begin()
			s.end(token, start, nil, false)
		}
	})
}

// A first violation after the prefix has advanced, with a gap above it and a
// later delivered instance: the window is relative to the current prefix.
func TestWindowSnapshotAdvancedPrefix(t *testing.T) {
	m, err := NewStateMachine(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint64{1, 2, 3, 5} {
		for from := ProcessID(1); from <= 3; from++ {
			mustHandle(t, m, from, PhaseReady, seq, "v")
		}
	}
	if m.delivered[1] != 3 {
		t.Fatalf("prefix %d, want 3", m.delivered[1])
	}
	mustHandle(t, m, 2, PhaseEcho, 3+WindowSize, "edge")
	reject(t, m, 4, PhaseEcho, 3+WindowSize+1)

	count, s := m.WindowViolations()
	want := WindowSnapshot{Sender: 4, Origin: 1, Phase: PhaseEcho, Sequence: 3 + WindowSize + 1, Prefix: 3, Open: 2,
		NextAllocated: false, LaterDelivered: 1, HighestDelivered: 5, Lowest: 4, LowestAllocated: false}
	if count != 1 || s == nil || fmt.Sprint(*s) != fmt.Sprint(want) {
		t.Fatalf("got %d %+v, want 1 %+v", count, s, want)
	}
}
