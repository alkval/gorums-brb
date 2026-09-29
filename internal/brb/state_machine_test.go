package brb

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSendFromDesignatedOriginEmitsOneEcho(t *testing.T) {
	machine := newTestMachine(t)
	id := BroadcastID{Origin: 1, Sequence: 1}

	if actions := handle(t, machine, 2, PhaseSend, id, "value"); len(actions) != 0 {
		t.Fatalf("SEND from non-origin emitted %v", actions)
	}
	actions := handle(t, machine, 1, PhaseSend, id, "value")
	assertSingleAction(t, actions, ActionMulticastEcho, "value")
	if actions := handle(t, machine, 1, PhaseSend, id, "value"); len(actions) != 0 {
		t.Fatalf("duplicate SEND emitted %v", actions)
	}
	if actions := handle(t, machine, 1, PhaseSend, id, "conflict"); len(actions) != 0 {
		t.Fatalf("conflicting SEND emitted a second ECHO: %v", actions)
	}
}

func TestEchoThresholdCountsDistinctSenders(t *testing.T) {
	machine := newTestMachine(t)
	id := BroadcastID{Origin: 1, Sequence: 1}

	for _, from := range []ProcessID{1, 1, 2} {
		if actions := handle(t, machine, from, PhaseEcho, id, "value"); len(actions) != 0 {
			t.Fatalf("ECHO from %d crossed threshold early: %v", from, actions)
		}
	}
	actions := handle(t, machine, 3, PhaseEcho, id, "value")
	assertSingleAction(t, actions, ActionMulticastReady, "value")
}

func TestReadyAmplificationAndDeliveryThresholds(t *testing.T) {
	machine := newTestMachine(t)
	id := BroadcastID{Origin: 1, Sequence: 1}

	if actions := handle(t, machine, 1, PhaseReady, id, "value"); len(actions) != 0 {
		t.Fatalf("one READY emitted %v", actions)
	}
	actions := handle(t, machine, 2, PhaseReady, id, "value")
	assertSingleAction(t, actions, ActionMulticastReady, "value")
	actions = handle(t, machine, 3, PhaseReady, id, "value")
	assertSingleAction(t, actions, ActionDeliver, "value")
	if actions := handle(t, machine, 4, PhaseReady, id, "value"); len(actions) != 0 {
		t.Fatalf("post-delivery READY emitted %v", actions)
	}
}

func TestConflictingValuesDoNotCauseTwoLocalReadyMessages(t *testing.T) {
	machine := newTestMachine(t)
	id := BroadcastID{Origin: 1, Sequence: 1}

	for _, from := range []ProcessID{1, 2, 3} {
		handle(t, machine, from, PhaseEcho, id, "first")
	}
	for _, from := range []ProcessID{1, 2, 3, 4} {
		if actions := handle(t, machine, from, PhaseEcho, id, "second"); len(actions) != 0 {
			t.Fatalf("conflicting ECHO emitted a second READY: %v", actions)
		}
	}
}

func TestBroadcastInstancesAreIndependent(t *testing.T) {
	machine := newTestMachine(t)
	for _, id := range []BroadcastID{{1, 1}, {1, 2}, {2, 1}} {
		actions := handle(t, machine, id.Origin, PhaseSend, id, "value")
		assertSingleAction(t, actions, ActionMulticastEcho, "value")
		if actions[0].ID != id {
			t.Fatalf("action has ID %+v, want %+v", actions[0].ID, id)
		}
	}
}

func TestRejectsInvalidConfigurationAndIdentifiers(t *testing.T) {
	for _, cfg := range [][2]int{{0, 0}, {-1, 0}, {4, -1}, {3, 1}} {
		if _, err := NewMachine(cfg[0], cfg[1]); err == nil {
			t.Fatalf("accepted invalid configuration %v", cfg)
		}
	}
	machine := newTestMachine(t)
	for _, msg := range []struct {
		from  ProcessID
		phase Phase
		id    BroadcastID
	}{
		{0, PhaseEcho, BroadcastID{1, 1}},
		{5, PhaseEcho, BroadcastID{1, 1}},
		{1, PhaseEcho, BroadcastID{0, 1}},
		{1, PhaseEcho, BroadcastID{5, 1}},
		{1, PhaseEcho, BroadcastID{1, 0}},
		{1, 0, BroadcastID{1, 1}},
		{1, PhaseReady + 1, BroadcastID{1, 1}},
	} {
		if _, err := machine.Handle(msg.from, msg.phase, msg.id, []byte("value")); err == nil {
			t.Fatalf("accepted invalid message %+v", msg)
		}
	}
	if len(machine.instances) != 0 {
		t.Fatal("invalid messages allocated instance state")
	}
}

func newTestMachine(t *testing.T) *Machine {
	t.Helper()
	machine, err := NewMachine(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	return machine
}

func handle(t *testing.T, machine *Machine, from ProcessID, phase Phase, id BroadcastID, value string) []Action {
	t.Helper()
	actions, err := machine.Handle(from, phase, id, []byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return actions
}

func assertSingleAction(t *testing.T, actions []Action, wantType ActionType, wantValue string) {
	t.Helper()
	if len(actions) != 1 {
		t.Fatalf("got %d actions, want 1: %v", len(actions), actions)
	}
	if actions[0].Type != wantType || string(actions[0].Value) != wantValue {
		t.Fatalf("got action %+v, want type %d value %q", actions[0], wantType, wantValue)
	}
}

func TestFirstVoteBoundsStorageAndDeliveryReleasesVotes(t *testing.T) {
	m := newTestMachine(t)
	id := BroadcastID{1, 1}
	for i := 0; i < 1000; i++ {
		handle(t, m, 1, PhaseEcho, id, fmt.Sprint(i))
		handle(t, m, 1, PhaseReady, id, fmt.Sprint(i))
	}
	s := m.instances[id]
	if len(s.echoes.counts) != 1 || len(s.readies.counts) != 1 {
		t.Fatal("stored conflicting votes from one sender")
	}
	for _, from := range []ProcessID{2, 3} {
		handle(t, m, from, PhaseReady, id, "0")
	}
	if !s.delivered || s.echoes.counts != nil || s.readies.counts != nil {
		t.Fatal("completed instance retained votes")
	}
	if actions := handle(t, m, 1, PhaseSend, id, "late"); len(actions) != 0 {
		t.Fatal("completed instance restarted")
	}
	// Wrong-origin SEND must not allocate an instance.
	handle(t, m, 2, PhaseSend, BroadcastID{1, 2}, "invalid")
	if len(m.instances) != 1 {
		t.Fatal("invalid SEND allocated state")
	}
}

func TestThresholdsAcrossMembershipSizes(t *testing.T) {
	for _, cfg := range [][2]int{{1, 0}, {3, 0}, {4, 1}, {5, 1}, {6, 1}, {7, 2}, {8, 2}, {10, 3}} {
		n, f := cfg[0], cfg[1]
		t.Run(fmt.Sprint(cfg), func(t *testing.T) {
			m, err := NewMachine(n, f)
			if err != nil {
				t.Fatal(err)
			}
			id := BroadcastID{1, 1}
			threshold := (n+f)/2 + 1
			for i := 1; i <= threshold; i++ {
				actions := handle(t, m, ProcessID(i), PhaseEcho, id, "v")
				if i < threshold && len(actions) != 0 {
					t.Fatal("early READY")
				}
				if i == threshold {
					assertSingleAction(t, actions, ActionMulticastReady, "v")
				}
			}
			m, _ = NewMachine(n, f)
			for i := 1; i <= 2*f+1; i++ {
				actions := handle(t, m, ProcessID(i), PhaseReady, id, "v")
				var ready, deliver bool
				for _, a := range actions {
					ready = ready || a.Type == ActionMulticastReady
					deliver = deliver || a.Type == ActionDeliver
				}
				if ready != (i == f+1) || deliver != (i == 2*f+1) {
					t.Fatalf("wrong actions at %d READYs: %v", i, actions)
				}
				if duplicate := handle(t, m, ProcessID(i), PhaseReady, id, "v"); len(duplicate) != 0 {
					t.Fatal("duplicate changed state")
				}
			}
		})
	}
}

func TestActionsOwnTheirPayload(t *testing.T) {
	m := newTestMachine(t)
	value := []byte("original")
	actions, err := m.Handle(1, PhaseSend, BroadcastID{1, 1}, value)
	if err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	assertSingleAction(t, actions, ActionMulticastEcho, "original")
}

func TestVotesForDifferentValuesDoNotCombine(t *testing.T) {
	m := newTestMachine(t)
	id := BroadcastID{1, 1}
	for _, phase := range []Phase{PhaseEcho, PhaseReady} {
		if actions := handle(t, m, 1, phase, id, "A"); len(actions) != 0 {
			t.Fatalf("first vote emitted %v", actions)
		}
		if actions := handle(t, m, 2, phase, id, "B"); len(actions) != 0 {
			t.Fatalf("different values combined: %v", actions)
		}
		// Changing a previous vote must not add evidence for another value.
		if actions := handle(t, m, 2, phase, id, "A"); len(actions) != 0 {
			t.Fatalf("conflicting vote counted twice: %v", actions)
		}
	}
	if actions := handle(t, m, 3, PhaseEcho, id, "A"); len(actions) != 0 {
		t.Fatalf("two matching ECHOs emitted %v", actions)
	}
	assertSingleAction(t, handle(t, m, 4, PhaseEcho, id, "A"), ActionMulticastReady, "A")
	if actions := handle(t, m, 3, PhaseReady, id, "A"); len(actions) != 0 {
		t.Fatalf("two matching READYs delivered or repeated READY: %v", actions)
	}
	assertSingleAction(t, handle(t, m, 4, PhaseReady, id, "A"), ActionDeliver, "A")
	for _, phase := range []Phase{PhaseSend, PhaseEcho, PhaseReady} {
		if actions := handle(t, m, 1, phase, id, "late"); len(actions) != 0 {
			t.Fatalf("delivered instance restarted on phase %d: %v", phase, actions)
		}
	}
}

// A small deterministic single-broadcast simulator: correct processes run Machine,
// faulty processes send only explicitly injected messages. Each event can be
// duplicated, and every queued event is eventually processed in random order.
type event struct {
	from, to ProcessID
	phase    Phase
	id       BroadcastID
	value    string
}

func simulate(t *testing.T, n, f int, correct []ProcessID, queue []event, seed int64) map[ProcessID]string {
	t.Helper()
	queue = append([]event(nil), queue...)
	machines := map[ProcessID]*Machine{}
	for _, id := range correct {
		m, err := NewMachine(n, f)
		if err != nil {
			t.Fatal(err)
		}
		machines[id] = m
	}
	delivered := map[ProcessID]string{}
	type actionKey struct {
		node   ProcessID
		action ActionType
		id     BroadcastID
	}
	emitted := map[actionKey]bool{}
	rng := rand.New(rand.NewSource(seed))
	steps := 0
	for len(queue) > 0 {
		steps++
		if steps > 10000 {
			t.Fatal("message storm")
		}
		index := rng.Intn(len(queue))
		e := queue[index]
		queue[index] = queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		m := machines[e.to]
		if m == nil {
			continue
		}
		repeats := 1 + rng.Intn(2)
		for range repeats {
			actions := handle(t, m, e.from, e.phase, e.id, e.value)
			for _, a := range actions {
				key := actionKey{e.to, a.Type, a.ID}
				if emitted[key] {
					t.Fatalf("node %d emitted action %d twice", e.to, a.Type)
				}
				emitted[key] = true
				if a.Type == ActionDeliver {
					delivered[e.to] = string(a.Value)
					continue
				}
				phase := PhaseEcho
				if a.Type == ActionMulticastReady {
					phase = PhaseReady
				}
				for _, to := range correct {
					queue = append(queue, event{e.to, to, phase, a.ID, string(a.Value)})
				}
			}
		}
	}
	var first string
	haveFirst := false
	for _, v := range delivered {
		if haveFirst && first != v {
			t.Fatal("correct processes delivered conflicting values")
		}
		first = v
		haveFirst = true
	}
	if len(delivered) > 0 && len(delivered) != len(correct) {
		t.Fatal("totality violated after draining reliable network")
	}
	return delivered
}

func TestCorrectSenderWithSilentFaultsAndReordering(t *testing.T) {
	for _, cfg := range [][2]int{{1, 0}, {4, 1}, {6, 1}, {7, 2}} {
		n, f := cfg[0], cfg[1]
		for seed := int64(0); seed < 30; seed++ {
			correct := []ProcessID{}
			queue := []event{}
			for i := 1; i <= n-f; i++ {
				correct = append(correct, ProcessID(i))
				queue = append(queue, event{1, ProcessID(i), PhaseSend, BroadcastID{1, 1}, "value"})
			}
			result := simulate(t, n, f, correct, queue, seed)
			if len(result) != len(correct) {
				t.Fatalf("validity failed for n=%d f=%d seed=%d", n, f, seed)
			}
			for _, v := range result {
				if v != "value" {
					t.Fatal("wrong value")
				}
			}
		}
	}
}

func TestEquivocatingOriginAndReadyAmplification(t *testing.T) {
	id := BroadcastID{1, 1}
	for seed := int64(0); seed < 30; seed++ {
		// Faulty origin gives A to two correct processes and B to the third.
		// Its selective ECHO gives node 2 a quorum. Its READY helps node 3
		// relay A. Node 4 must eventually deliver A despite having echoed B.
		queue := []event{
			{1, 2, PhaseSend, id, "A"}, {1, 3, PhaseSend, id, "A"}, {1, 4, PhaseSend, id, "B"},
			{1, 2, PhaseEcho, id, "A"}, {1, 3, PhaseReady, id, "A"},
		}
		result := simulate(t, 4, 1, []ProcessID{2, 3, 4}, queue, seed)
		if len(result) != 3 {
			t.Fatalf("amplification failed, seed %d", seed)
		}
		for _, v := range result {
			if v != "A" {
				t.Fatal("unexpected value")
			}
		}
		// Without the faulty process's extra votes, delivery need not happen.
		result = simulate(t, 4, 1, []ProcessID{2, 3, 4}, queue[:3], seed)
		if len(result) != 0 {
			t.Fatal("delivered without sufficient evidence")
		}
	}
}
