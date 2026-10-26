package brb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func optimisticMachine(t *testing.T, n, f int, self ProcessID) *StateMachine {
	t.Helper()
	m, err := NewOptimisticStateMachine(n, f, self)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestOptimisticThresholds(t *testing.T) {
	id := BroadcastID{1, 1} // ECHO senders 1..3, Q-ECHO senders 1..2, READY senders 1..3.
	m := optimisticMachine(t, 4, 1, 1)
	check := func(from ProcessID, phase Phase, value string, want ActionType) {
		t.Helper()
		a, err := m.Handle(from, phase, id, []byte(value))
		if err != nil {
			t.Fatal(err)
		}
		if want == 0 {
			if len(a) != 0 {
				t.Fatalf("unexpected actions: %v", a)
			}
		} else if len(a) != 1 || a[0].Type != want || string(a[0].Value) != value {
			t.Fatalf("actions=%v, want %v", a, want)
		}
	}
	check(2, PhasePropose, "bad", 0)
	check(1, PhasePropose, "v", ActionMulticastEcho)
	check(1, PhasePropose, "other", 0)
	check(4, PhaseEcho, "v", 0) // Not in the ECHO-sending committee.
	check(1, PhaseEcho, "v", 0)
	check(1, PhaseEcho, "v", 0)
	check(2, PhaseEcho, "v", 0)
	check(3, PhaseEcho, "v", ActionMulticastQEcho)
	check(1, PhaseQEcho, "v", 0)
	check(1, PhaseQEcho, "other", 0)
	check(3, PhaseQEcho, "v", 0) // Not in the Q-ECHO-sending committee.
	check(2, PhaseQEcho, "v", ActionMulticastReady)
	check(4, PhaseReady, "v", 0)
	check(1, PhaseReady, "v", 0)
	check(2, PhaseReady, "v", 0)
	check(3, PhaseReady, "v", ActionDeliver)
	check(3, PhaseReady, "v", 0)
	if len(m.instances) != 0 || m.delivered[1] != 1 {
		t.Fatal("delivery did not compact")
	}
}

func TestOptimisticHasNoReadyAmplification(t *testing.T) {
	id := BroadcastID{1, 1}
	// A faulty READY sender can help one replica deliver and withhold its vote
	// from another. Without recovery the latter stays stuck, even with f+1 READYs.
	a := optimisticMachine(t, 4, 1, 1)
	b := optimisticMachine(t, 4, 1, 4)
	for _, m := range []*StateMachine{a, b} {
		for _, from := range []ProcessID{1, 2} {
			actions, err := m.Handle(from, PhaseReady, id, []byte("v"))
			if err != nil || len(actions) != 0 {
				t.Fatalf("amplified READY: %v %v", actions, err)
			}
		}
	}
	actions, err := a.Handle(3, PhaseReady, id, []byte("v"))
	if err != nil || len(actions) != 1 || actions[0].Type != ActionDeliver {
		t.Fatal("expected local delivery")
	}
	if b.delivered[1] != 0 {
		t.Fatal("unexpected delivery without the last READY")
	}
}

func TestOptimisticSilentOrConflictingEchoStalls(t *testing.T) {
	for _, conflicting := range []bool{false, true} {
		m := optimisticMachine(t, 4, 1, 1)
		id := BroadcastID{1, 1}
		// Two correct ECHOs cannot replace the third selected member.
		for _, from := range []ProcessID{1, 2} {
			a, err := m.Handle(from, PhaseEcho, id, []byte("v"))
			if err != nil || len(a) != 0 {
				t.Fatalf("premature quorum: %v %v", a, err)
			}
		}
		if conflicting {
			for _, value := range []string{"other", "v"} {
				a, err := m.Handle(3, PhaseEcho, id, []byte(value))
				if err != nil || len(a) != 0 {
					t.Fatalf("conflicting voter completed quorum: %v %v", a, err)
				}
			}
		}
		if m.instances[id].qEchoSent || m.instances[id].delivered {
			t.Fatal("unexpected progress")
		}
	}
}

func TestOptimisticRejectsInvalidIdentityAndHonorsWindow(t *testing.T) {
	for _, self := range []ProcessID{0, 5} {
		if _, err := NewOptimisticStateMachine(4, 1, self); err == nil {
			t.Fatal("accepted invalid identity")
		}
	}
	if _, err := NewOptimisticStateMachine(6, 2, 1); err == nil {
		t.Fatal("accepted invalid fault bound")
	}
	m := optimisticMachine(t, 4, 1, 1)
	_, err := m.Handle(1, PhasePropose, BroadcastID{1, WindowSize + 1}, nil)
	if err != ErrOutsideWindow || len(m.instances) != 0 {
		t.Fatalf("window check: %v", err)
	}
	for seq := uint64(1); seq <= WindowSize+1; seq++ {
		id := BroadcastID{1, seq}
		for from := ProcessID(1); from <= 4; from++ {
			if m.committeeContains(id, from, 3) {
				if _, err := m.Handle(from, PhaseReady, id, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if len(m.instances) != 0 || m.delivered[1] != WindowSize+1 {
		t.Fatal("window did not advance")
	}
}

func TestOptimisticRotatingCommittees(t *testing.T) {
	for _, sizes := range [][2]int{{1, 0}, {4, 1}, {5, 1}, {6, 1}, {7, 2}, {13, 1}} {
		n, f := sizes[0], sizes[1]
		t.Run(fmt.Sprintf("n%d-f%d", n, f), func(t *testing.T) {
			m := optimisticMachine(t, n, f, 1)
			for _, size := range []int{(n+f)/2 + 1, f + 1, 2*f + 1} {
				counts := make([]int, n)
				for seq := uint64(1); seq <= uint64(n); seq++ {
					members := 0
					for p := 1; p <= n; p++ {
						if m.committeeContains(BroadcastID{1, seq}, ProcessID(p), size) {
							counts[p-1]++
							members++
						}
					}
					if members != size {
						t.Fatal("incorrect committee size")
					}
				}
				for _, count := range counts {
					if count != size {
						t.Fatal("unbalanced rotation")
					}
				}
			}
		})
	}
}

func TestOptimisticReorderedBroadcasts(t *testing.T) {
	for _, sizes := range [][2]int{{1, 0}, {4, 1}, {5, 1}, {6, 1}, {7, 2}, {13, 1}} {
		n, f := sizes[0], sizes[1]
		t.Run(fmt.Sprintf("n%d-f%d", n, f), func(t *testing.T) {
			type message struct {
				from, to ProcessID
				phase    Phase
				id       BroadcastID
				value    []byte
			}
			machines := make([]*StateMachine, n)
			for i := range machines {
				machines[i] = optimisticMachine(t, n, f, ProcessID(i+1))
			}
			var queue []message
			for origin := 1; origin <= n; origin++ {
				for seq := uint64(1); seq <= 3; seq++ {
					id := BroadcastID{ProcessID(origin), seq}
					for p := 1; p <= n; p++ {
						queue = append(queue, message{id.Origin, ProcessID(p), PhasePropose, id, fmt.Append(nil, id)})
					}
				}
			}
			rng := rand.New(rand.NewPCG(42, 17))
			delivered := make(map[[3]uint64]bool)
			for len(queue) > 0 {
				i := rng.IntN(len(queue))
				msg := queue[i]
				queue[i] = queue[len(queue)-1]
				queue = queue[:len(queue)-1]
				actions, err := machines[msg.to-1].Handle(msg.from, msg.phase, msg.id, msg.value)
				if err != nil {
					t.Fatal(err)
				}
				for _, a := range actions {
					if a.Type == ActionDeliver {
						key := [3]uint64{uint64(msg.to), uint64(a.ID.Origin), a.ID.Sequence}
						if delivered[key] || string(a.Value) != fmt.Sprint(a.ID) {
							t.Fatal("duplicate or incorrect delivery")
						}
						delivered[key] = true
						continue
					}
					phase := PhaseReady
					size := n
					if a.Type == ActionMulticastEcho {
						phase = PhaseEcho
						size = f + 1
					}
					if a.Type == ActionMulticastQEcho {
						phase = PhaseQEcho
						size = 2*f + 1
					}
					for p := 1; p <= n; p++ {
						if machines[0].committeeContains(a.ID, ProcessID(p), size) {
							out := message{msg.to, ProcessID(p), phase, a.ID, a.Value}
							queue = append(queue, out)
							if rng.IntN(3) == 0 {
								queue = append(queue, out)
							}
						}
					}
				}
			}
			if len(delivered) != n*n*3 {
				t.Fatalf("got %d deliveries, want %d", len(delivered), n*n*3)
			}
		})
	}
}

func TestOptimisticGorumsDelivery(t *testing.T) {
	systems, stop, err := gorums.NewLocalServers(4, gorums.WithLocalDialOptions(gorums.WithGRPCDialOptions(
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	nodes := make([]*Replica, 4)
	deliveries := make([]chan Delivery, 4)
	for i, sys := range systems {
		deliveries[i] = make(chan Delivery, 100)
		nodes[i], err = NewOptimisticReplica(ctx, sys, 1, func(d Delivery) { deliveries[i] <- d })
		if err != nil {
			t.Fatal(err)
		}
	}
	// Verify the adapter actually selects smaller recipient sets. A multicast
	// to everyone could still deliver correctly while defeating the optimisation.
	for _, tc := range []struct {
		action ActionType
		seq    uint64
		want   string
	}{
		{ActionMulticastEcho, 1, "[1 2]"},
		{ActionMulticastQEcho, 1, "[1 2 3]"},
		{ActionMulticastReady, 1, "[1 2 3 4]"},
		{ActionMulticastEcho, 2, "[2 3]"},
		{ActionMulticastQEcho, 2, "[2 3 4]"},
	} {
		var ids []uint32
		for _, peer := range nodes[0].recipients(Action{Type: tc.action, ID: BroadcastID{1, tc.seq}}) {
			ids = append(ids, peer.ID())
		}
		if fmt.Sprint(ids) != tc.want {
			t.Fatalf("recipients=%v, want %s", ids, tc.want)
		}
	}
	serve(t, systems)
	for seq := uint64(1); seq <= 4; seq++ {
		for origin, node := range nodes {
			if err := node.Broadcast(seq, []byte("hello")); err != nil {
				t.Fatal(err)
			}
			for _, ch := range deliveries {
				awaitDelivery(t, ch, BroadcastID{ProcessID(origin + 1), seq}, "hello")
			}
		}
	}
	// Delivery needs a vote from every committee member, so every assigned
	// sender has counted its PROPOSE, ECHO, Q-ECHO and READY. Message sizes are
	// equal here because all IDs and senders encode in one varint byte.
	size := uint64(proto.Size(nodes[0].message(BroadcastID{1, 1}, []byte("hello"))))
	q := (4+1)/2 + 1
	want := make([][PhaseQEcho + 1]Traffic, 4)
	add := func(sender ProcessID, phase Phase, id BroadcastID, recipients int) {
		w := &want[sender-1][phase]
		w.Calls++
		for p := 1; p <= 4; p++ {
			if recipients < 4 && !nodes[0].machine.committeeContains(id, ProcessID(p), recipients) {
				continue
			}
			if ProcessID(p) == sender {
				w.LocalMessages++
				w.LocalBytes += size
			} else {
				w.RemoteMessages++
				w.RemoteBytes += size
			}
		}
	}
	for origin := ProcessID(1); origin <= 4; origin++ {
		for seq := uint64(1); seq <= 4; seq++ {
			id := BroadcastID{origin, seq}
			add(origin, PhasePropose, id, 4)
			for p := ProcessID(1); p <= 4; p++ {
				m := nodes[0].machine
				if m.committeeContains(id, p, q) {
					add(p, PhaseEcho, id, 2)
				}
				if m.committeeContains(id, p, 2) {
					add(p, PhaseQEcho, id, 3)
				}
				if m.committeeContains(id, p, 3) {
					add(p, PhaseReady, id, 4)
				}
			}
		}
	}
	for i, node := range nodes {
		for phase := PhasePropose; phase <= PhaseQEcho; phase++ {
			if got := node.Traffic(phase); got != want[i][phase] {
				t.Fatalf("node %d phase %d traffic = %+v, want %+v", i+1, phase, got, want[i][phase])
			}
		}
	}
}

func TestRejectMismatchedVariant(t *testing.T) {
	// Every variant, plus a value unknown to this version.
	variants := []brbv1.Variant{brbv1.Variant_VARIANT_BRACHA, brbv1.Variant_VARIANT_OPTIMISTIC,
		brbv1.Variant_VARIANT_BRACHA_READY_DIGEST, brbv1.Variant(7)}
	digest, err := NewReadyDigestStateMachine(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []*StateMachine{newTestMachine(t), optimisticMachine(t, 4, 1, 1), digest} {
		node := &Replica{self: 1, machine: m, ctx: t.Context()}
		msg := phaseMessage(BroadcastID{1, 1}, []byte("v"))
		for _, v := range variants {
			if v == node.variant() {
				continue
			}
			msg.SetVariant(v)
			node.handleFrom(1, PhaseEcho, msg)
			if len(m.instances) != 0 {
				t.Fatalf("replica of variant %v accepted %v", node.variant(), v)
			}
		}
		// The same vote from the same member is accepted with the right variant.
		// One ECHO is below every threshold, so no multicast is attempted.
		msg.SetVariant(node.variant())
		node.handleFrom(1, PhaseEcho, msg)
		if len(m.instances) != 1 {
			t.Fatal("rejected a matching protocol variant")
		}
	}
}
