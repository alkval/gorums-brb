package brb

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWindowCompactsOnlyContiguousDeliveries(t *testing.T) {
	m, err := NewStateMachine(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Deliver 2 first. It must remain remembered while 1 is missing.
	handle(t, m, 1, PhaseReady, BroadcastID{1, 2}, "two")
	if m.delivered[1] != 0 || len(m.instances) != 1 {
		t.Fatal("skipped a gap")
	}
	if actions := handle(t, m, 1, PhaseReady, BroadcastID{1, 2}, "conflict"); len(actions) != 0 {
		t.Fatal("duplicate delivery")
	}
	handle(t, m, 1, PhaseReady, BroadcastID{1, 1}, "one")
	if m.delivered[1] != 2 || len(m.instances) != 0 {
		t.Fatal("did not compact delivered prefix")
	}
	for _, seq := range []uint64{1, 2} {
		if actions := handle(t, m, 1, PhasePropose, BroadcastID{1, seq}, "late"); len(actions) != 0 {
			t.Fatal("restarted compacted instance")
		}
	}
}

func TestWindowRejectsFutureAndOversizedMessagesWithoutStorage(t *testing.T) {
	m := newTestMachine(t)
	for _, seq := range []uint64{WindowSize + 1, ^uint64(0)} {
		if _, err := m.Handle(1, PhaseEcho, BroadcastID{1, seq}, []byte("x")); !errors.Is(err, ErrOutsideWindow) {
			t.Fatalf("future sequence: %v", err)
		}
	}
	if _, err := m.Handle(1, PhaseEcho, BroadcastID{1, 1}, make([]byte, MaxPayloadSize+1)); err == nil {
		t.Fatal("accepted oversized payload")
	}
	if len(m.instances) != 0 {
		t.Fatal("invalid messages allocated instances")
	}
	handle(t, m, 1, PhaseEcho, BroadcastID{1, WindowSize}, "boundary")
	if len(m.instances) != 1 {
		t.Fatal("rejected window boundary")
	}
}

func TestWindowBoundsLongRunAndKeepsOriginsIndependent(t *testing.T) {
	m, err := NewStateMachine(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 3*WindowSize; seq++ {
		id := BroadcastID{1, seq}
		if err := m.reserve(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		handle(t, m, 1, PhaseReady, id, "x")
		if len(m.instances) != 0 {
			t.Fatal("completed run retained instances")
		}
	}
	if err := m.reserve(t.Context(), BroadcastID{1, 1}); err == nil {
		t.Fatal("reused compacted sequence")
	}
	if m.delivered[2] != 0 {
		t.Fatal("advanced another origin")
	}
	handle(t, m, 2, PhaseReady, BroadcastID{2, 1}, "independent")
}

func TestLocalWindowBackpressureAndCancellation(t *testing.T) {
	m, err := NewStateMachine(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { result <- m.reserve(ctx, BroadcastID{1, WindowSize + 1}) }()
	select {
	case err := <-result:
		t.Fatalf("future reservation did not wait: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	handle(t, m, 1, PhaseReady, BroadcastID{1, 1}, "one")
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("reservation did not resume")
	}
	cancel()
	if err := m.reserve(ctx, BroadcastID{1, WindowSize + 2}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled reservation: %v", err)
	}
}
