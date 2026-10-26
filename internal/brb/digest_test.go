package brb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func digestMachine(t *testing.T, n, f int) *StateMachine {
	t.Helper()
	m, err := NewReadyDigestStateMachine(n, f)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func sum(value string) []byte {
	d := sha256.Sum256([]byte(value))
	return d[:]
}

func readyVote(t *testing.T, m *StateMachine, from ProcessID, id BroadcastID, digest []byte) []Action {
	t.Helper()
	actions, err := m.HandleReadyDigest(from, id, digest)
	if err != nil {
		t.Fatal(err)
	}
	return actions
}

func countType(actions []Action, typ ActionType) int {
	n := 0
	for _, a := range actions {
		if a.Type == typ {
			n++
		}
	}
	return n
}

func payloads(m *StateMachine, id BroadcastID) int {
	if s := m.instances[id]; s != nil {
		return len(s.payloads)
	}
	return 0
}

func TestDigestThresholdsNeedPayload(t *testing.T) {
	for _, cfg := range [][2]int{{1, 0}, {3, 0}, {4, 1}, {5, 1}, {6, 1}, {7, 2}, {8, 2}, {10, 3}} {
		n, f := cfg[0], cfg[1]
		t.Run(fmt.Sprint(cfg), func(t *testing.T) {
			id := BroadcastID{1, 1}
			// An ECHO quorum of full values triggers one READY carrying only the digest.
			m := digestMachine(t, n, f)
			threshold := (n+f)/2 + 1
			for i := 1; i <= threshold; i++ {
				actions := handle(t, m, ProcessID(i), PhaseEcho, id, "v")
				if (i == threshold) != (countType(actions, ActionMulticastReady) == 1) {
					t.Fatalf("READY after %d ECHOs: %v", i, actions)
				}
				if i == threshold && (actions[0].Value != nil || !bytes.Equal(actions[0].Digest, sum("v"))) {
					t.Fatalf("READY action %+v, want only the digest", actions[0])
				}
			}
			// READY amplification at f+1 and delivery at 2f+1, but only with the value.
			m = digestMachine(t, n, f)
			for i := 1; i <= 2*f+1; i++ {
				actions := readyVote(t, m, ProcessID(i), id, sum("v"))
				if (i == f+1) != (countType(actions, ActionMulticastReady) == 1) {
					t.Fatalf("amplification after %d READYs: %v", i, actions)
				}
				if countType(actions, ActionDeliver) != 0 {
					t.Fatal("delivered without the value")
				}
			}
			actions := handle(t, m, 1, PhasePropose, id, "v")
			if countType(actions, ActionDeliver) != 1 {
				t.Fatalf("matching PROPOSE did not deliver: %v", actions)
			}
		})
	}
}

// With the value already stored, delivery must wait for exactly 2f+1 distinct
// READYs. Withholding the value until the end cannot tell 2f from 2f+1.
func TestDigestDeliversAtThresholdWithPayloadFirst(t *testing.T) {
	for _, cfg := range [][2]int{{1, 0}, {3, 0}, {4, 1}, {5, 1}, {6, 1}, {7, 2}, {8, 2}, {10, 3}} {
		n, f := cfg[0], cfg[1]
		t.Run(fmt.Sprint(cfg), func(t *testing.T) {
			m := digestMachine(t, n, f)
			id := BroadcastID{1, 1}
			handle(t, m, 1, PhasePropose, id, "v")
			for i := 1; i <= 2*f+1; i++ {
				actions := readyVote(t, m, ProcessID(i), id, sum("v"))
				if got, want := countType(actions, ActionDeliver), map[bool]int{false: 0, true: 1}[i == 2*f+1]; got != want {
					t.Fatalf("%d deliveries after %d READYs, want %d", got, i, want)
				}
				if i < 2*f+1 {
					// A repeated READY from the same sender must not count again.
					if actions := readyVote(t, m, ProcessID(i), id, sum("v")); len(actions) != 0 {
						t.Fatalf("duplicate READY acted: %v", actions)
					}
				}
			}
		})
	}
}

func TestDigestQuorumBeforePayloadDeliversOnce(t *testing.T) {
	for _, supply := range []Phase{PhaseEcho, PhasePropose} {
		t.Run(fmt.Sprint(supply), func(t *testing.T) {
			m := digestMachine(t, 4, 1)
			id := BroadcastID{1, 1}
			for _, from := range []ProcessID{2, 3, 4} {
				if actions := readyVote(t, m, from, id, sum("v")); countType(actions, ActionDeliver) != 0 {
					t.Fatal("delivered without the value")
				}
			}
			actions := handle(t, m, 1, supply, id, "v")
			if countType(actions, ActionDeliver) != 1 {
				t.Fatalf("matching value did not deliver: %v", actions)
			}
			for _, a := range actions {
				if a.Type == ActionDeliver && string(a.Value) != "v" {
					t.Fatalf("delivered %q", a.Value)
				}
			}
			for _, phase := range []Phase{PhasePropose, PhaseEcho} {
				if actions := handle(t, m, 2, phase, id, "v"); len(actions) != 0 {
					t.Fatalf("delivered instance acted again: %v", actions)
				}
			}
		})
	}
}

func TestDigestWrongPayloadThenMatchingPayload(t *testing.T) {
	m := digestMachine(t, 4, 1)
	id := BroadcastID{1, 1}
	for _, from := range []ProcessID{1, 2, 3} {
		readyVote(t, m, from, id, sum("v"))
	}
	// Sender 2's first ECHO carries a different value. It does not deliver.
	if actions := handle(t, m, 2, PhaseEcho, id, "other"); countType(actions, ActionDeliver) != 0 {
		t.Fatal("delivered a value whose digest does not match")
	}
	// A replacement from the same sender is ignored and stores nothing.
	before := payloads(m, id)
	if actions := handle(t, m, 2, PhaseEcho, id, "v"); len(actions) != 0 || payloads(m, id) != before {
		t.Fatalf("replacement ECHO was accepted: %v", actions)
	}
	// The first ECHO from another sender supplies the matching value.
	if actions := handle(t, m, 3, PhaseEcho, id, "v"); countType(actions, ActionDeliver) != 1 {
		t.Fatalf("matching ECHO did not deliver: %v", actions)
	}
}

func TestDigestAmplifiesWithoutPayload(t *testing.T) {
	m := digestMachine(t, 4, 1)
	id := BroadcastID{1, 1}
	readyVote(t, m, 2, id, sum("v"))
	actions := readyVote(t, m, 3, id, sum("v"))
	if len(actions) != 1 || actions[0].Type != ActionMulticastReady || !bytes.Equal(actions[0].Digest, sum("v")) {
		t.Fatalf("no READY amplification without a value: %v", actions)
	}
	if actions := readyVote(t, m, 4, id, sum("v")); len(actions) != 0 {
		t.Fatalf("pending quorum acted: %v", actions)
	}
	if s := m.instances[id]; s == nil || s.delivered {
		t.Fatal("instance delivered or vanished without the value")
	}
	if actions := handle(t, m, 4, PhaseEcho, id, "v"); countType(actions, ActionDeliver) != 1 {
		t.Fatalf("matching value did not deliver: %v", actions)
	}
}

func TestDigestVotesDoNotCombine(t *testing.T) {
	m := digestMachine(t, 4, 1)
	id := BroadcastID{1, 1}
	readyVote(t, m, 1, id, sum("A"))
	readyVote(t, m, 2, id, sum("B"))
	// Duplicate and changed votes from sender 2 add nothing.
	for _, d := range [][]byte{sum("B"), sum("A")} {
		if actions := readyVote(t, m, 2, id, d); len(actions) != 0 {
			t.Fatalf("repeated READY counted: %v", actions)
		}
	}
	if s := m.instances[id]; s.readies.counts[string(sum("A"))] != 1 || s.readies.counts[string(sum("B"))] != 1 {
		t.Fatal("digest votes were combined or double counted")
	}
	// ECHO votes for different values do not combine either.
	handle(t, m, 1, PhaseEcho, id, "A")
	handle(t, m, 2, PhaseEcho, id, "B")
	if actions := handle(t, m, 3, PhaseEcho, id, "A"); countType(actions, ActionMulticastReady) != 0 {
		t.Fatal("two matching ECHOs triggered READY")
	}
}

func TestDigestFiltersRepeatedPayloads(t *testing.T) {
	m := digestMachine(t, 4, 1)
	id := BroadcastID{1, 1}
	handle(t, m, 1, PhasePropose, id, "p")
	for i := range 10 {
		handle(t, m, 1, PhasePropose, id, fmt.Sprint("propose", i))
		handle(t, m, 2, PhaseEcho, id, fmt.Sprint("echo", i))
	}
	// One PROPOSE and sender 2's first ECHO: two values, never more than n+1.
	if got := payloads(m, id); got != 2 {
		t.Fatalf("stored %d values, want 2", got)
	}
	for _, from := range []ProcessID{1, 3, 4} {
		handle(t, m, from, PhaseEcho, id, fmt.Sprint("first", from))
	}
	if got := payloads(m, id); got > 4+1 {
		t.Fatalf("stored %d values, more than n+1", got)
	}
}

func TestDigestValidation(t *testing.T) {
	m := digestMachine(t, 4, 1)
	id := BroadcastID{1, 1}
	for _, d := range [][]byte{nil, make([]byte, DigestSize-1), make([]byte, DigestSize+1)} {
		if _, err := m.HandleReadyDigest(2, id, d); err == nil {
			t.Fatalf("accepted a %d-byte digest", len(d))
		}
	}
	if _, err := m.Handle(2, PhaseReady, id, []byte("v")); err == nil {
		t.Fatal("accepted a full-value READY in the digest variant")
	}
	if _, err := m.Handle(2, PhaseQEcho, id, []byte("v")); err == nil {
		t.Fatal("accepted Q-ECHO in the digest variant")
	}
	if _, err := m.HandleReadyDigest(0, id, sum("v")); err == nil {
		t.Fatal("accepted a non-member sender")
	}
	if len(m.instances) != 0 {
		t.Fatal("invalid messages allocated state")
	}
	if _, err := newTestMachine(t).HandleReadyDigest(2, id, sum("v")); err == nil {
		t.Fatal("ordinary Bracha accepted a READY digest")
	}
}

func TestDigestEdgeValues(t *testing.T) {
	for _, value := range []string{"", "x", string(make([]byte, MaxPayloadSize))} {
		t.Run(fmt.Sprint(len(value)), func(t *testing.T) {
			m := digestMachine(t, 4, 1)
			id := BroadcastID{1, 1}
			for _, from := range []ProcessID{2, 3, 4} {
				readyVote(t, m, from, id, sum(value))
			}
			actions := handle(t, m, 1, PhasePropose, id, value)
			if countType(actions, ActionDeliver) != 1 {
				t.Fatalf("value of %d bytes did not deliver", len(value))
			}
			for _, a := range actions {
				if a.Type == ActionDeliver && string(a.Value) != value {
					t.Fatal("delivered a different value")
				}
			}
		})
	}
}

func TestDigestInstancesWithEqualValuesStayIndependent(t *testing.T) {
	m := digestMachine(t, 1, 0)
	a, b := BroadcastID{1, 1}, BroadcastID{1, 2}
	readyVote(t, m, 1, a, sum("same"))
	// Instance b's value does not complete instance a.
	if actions := handle(t, m, 1, PhaseEcho, b, "same"); countType(actions, ActionDeliver) != 0 {
		t.Fatal("instance b delivered without its own READY")
	}
	if actions := handle(t, m, 1, PhasePropose, a, "same"); countType(actions, ActionDeliver) != 1 {
		t.Fatal("instance a did not deliver")
	}
	readyVote(t, m, 1, b, sum("same"))
	if m.delivered[1] != 2 || len(m.instances) != 0 {
		t.Fatalf("prefix %d with %d instances, want 2 and 0", m.delivered[1], len(m.instances))
	}
	for _, id := range []BroadcastID{a, b} {
		if actions := handle(t, m, 1, PhasePropose, id, "same"); len(actions) != 0 {
			t.Fatal("compacted instance acted again")
		}
	}
}

func TestDigestOwnsPayloads(t *testing.T) {
	m := digestMachine(t, 4, 1)
	id := BroadcastID{1, 1}
	value := []byte("original")
	if _, err := m.Handle(1, PhasePropose, id, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	var delivered []byte
	for _, from := range []ProcessID{2, 3, 4} {
		for _, a := range readyVote(t, m, from, id, sum("original")) {
			if a.Type == ActionDeliver {
				delivered = a.Value
			}
		}
	}
	if string(delivered) != "original" {
		t.Fatalf("delivered %q after the caller changed its buffer", delivered)
	}
}

// A pending instance is not delivered, so the delivered prefix cannot pass it.
func TestDigestPendingInstanceHoldsWindow(t *testing.T) {
	m := digestMachine(t, 1, 0)
	gap := BroadcastID{1, 1}
	readyVote(t, m, 1, gap, sum("one")) // quorum, no value yet
	for seq := uint64(2); seq <= 5; seq++ {
		id := BroadcastID{1, seq}
		handle(t, m, 1, PhasePropose, id, "later")
		readyVote(t, m, 1, id, sum("later"))
	}
	if m.delivered[1] != 0 {
		t.Fatalf("prefix advanced past a pending instance to %d", m.delivered[1])
	}
	if _, err := m.HandleReadyDigest(1, BroadcastID{1, WindowSize + 1}, sum("x")); !errors.Is(err, ErrOutsideWindow) {
		t.Fatalf("out-of-window READY: %v", err)
	}
	if m.instances[BroadcastID{1, WindowSize + 1}] != nil {
		t.Fatal("out-of-window READY allocated state")
	}
	if actions := handle(t, m, 1, PhasePropose, gap, "one"); countType(actions, ActionDeliver) != 1 {
		t.Fatal("supplying the missing value did not deliver")
	}
	if m.delivered[1] != 5 || len(m.instances) != 0 {
		t.Fatalf("prefix %d with %d instances, want 5 and 0", m.delivered[1], len(m.instances))
	}
	if actions := readyVote(t, m, 1, gap, sum("one")); len(actions) != 0 {
		t.Fatal("replay reopened a compacted instance")
	}
}

func TestDigestLongRunStaysBounded(t *testing.T) {
	m := digestMachine(t, 2, 0)
	for seq := uint64(1); seq <= 100_000; seq++ {
		id := BroadcastID{1, seq}
		if err := m.reserve(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		handle(t, m, 1, PhasePropose, id, "x")
		readyVote(t, m, 1, id, sum("x"))
		if len(m.instances) != 0 {
			t.Fatal("completed broadcast retained state")
		}
	}
	if m.delivered[1] != 100_000 {
		t.Fatalf("prefix %d", m.delivered[1])
	}
}

// One correct ECHO carrying the value is enough, even when it arrives last.
func TestDigestSinglePayloadSourceSuffices(t *testing.T) {
	m := digestMachine(t, 4, 1)
	id := BroadcastID{1, 1}
	for _, from := range []ProcessID{1, 2, 3} {
		readyVote(t, m, from, id, sum("v"))
	}
	if actions := handle(t, m, 4, PhaseEcho, id, "v"); countType(actions, ActionDeliver) != 1 {
		t.Fatal("the only value source did not complete delivery")
	}
}

func TestDigestSimulations(t *testing.T) {
	id := BroadcastID{1, 1}
	newDigest := func(n, f int) (*StateMachine, error) { return NewReadyDigestStateMachine(n, f) }
	for _, cfg := range [][2]int{{1, 0}, {4, 1}, {6, 1}, {7, 2}} {
		n, f := cfg[0], cfg[1]
		for seed := range int64(30) {
			var correct []ProcessID
			var queue []event
			for i := 1; i <= n-f; i++ {
				correct = append(correct, ProcessID(i))
				queue = append(queue, event{1, ProcessID(i), PhasePropose, id, "value"})
			}
			result := simulateWith(t, newDigest, n, f, correct, queue, seed)
			if len(result) != len(correct) {
				t.Fatalf("validity failed for n=%d f=%d seed=%d", n, f, seed)
			}
			for _, v := range result {
				if v != "value" {
					t.Fatalf("delivered %q", v)
				}
			}
		}
	}
	for seed := range int64(30) {
		// The faulty origin gives its extra ECHO and READY for A only to node
		// 2. Node 2 then holds two READYs, one short of delivery. A threshold
		// of 2f would let it deliver alone and violate totality.
		queue := []event{
			{1, 2, PhasePropose, id, "A"}, {1, 3, PhasePropose, id, "A"}, {1, 4, PhasePropose, id, "B"},
			{1, 2, PhaseEcho, id, "A"}, {1, 2, PhaseReady, id, string(sum("A"))},
		}
		if result := simulateWith(t, newDigest, 4, 1, []ProcessID{2, 3, 4}, queue, seed); len(result) != 0 {
			t.Fatalf("delivered without a READY quorum, seed %d: %v", seed, result)
		}
	}
	for seed := range int64(30) {
		// The faulty origin proposes A to nodes 2 and 3 and B to node 4, adds
		// an ECHO for A at node 2 and a READY digest for A at node 3. Node 4
		// echoed B, yet must still deliver A, with A's value from correct ECHOs.
		queue := []event{
			{1, 2, PhasePropose, id, "A"}, {1, 3, PhasePropose, id, "A"}, {1, 4, PhasePropose, id, "B"},
			{1, 2, PhaseEcho, id, "A"}, {1, 3, PhaseReady, id, string(sum("A"))},
		}
		result := simulateWith(t, newDigest, 4, 1, []ProcessID{2, 3, 4}, queue, seed)
		if len(result) != 3 {
			t.Fatalf("totality failed, seed %d: %v", seed, result)
		}
		for _, v := range result {
			if v != "A" {
				t.Fatalf("delivered %q", v)
			}
		}
	}
}

func TestReadyDigestMessageFields(t *testing.T) {
	id := BroadcastID{1, 1}
	msg := func(variant brbv1.Variant, value, digest []byte) *brbv1.PhaseMessage {
		m := phaseMessage(id, value)
		m.SetVariant(variant)
		m.SetDigest(digest)
		return m
	}
	digestVariant := brbv1.Variant_VARIANT_BRACHA_READY_DIGEST
	for name, tc := range map[string]struct {
		machine *StateMachine
		phase   Phase
		msg     *brbv1.PhaseMessage
	}{
		"value in a digest READY":   {digestMachine(t, 4, 1), PhaseReady, msg(digestVariant, []byte("v"), sum("v"))},
		"short READY digest":        {digestMachine(t, 4, 1), PhaseReady, msg(digestVariant, nil, sum("v")[:31])},
		"digest in PROPOSE":         {digestMachine(t, 4, 1), PhasePropose, msg(digestVariant, []byte("v"), sum("v"))},
		"digest in ECHO":            {digestMachine(t, 4, 1), PhaseEcho, msg(digestVariant, []byte("v"), sum("v"))},
		"digest in a Bracha ECHO":   {newTestMachine(t), PhaseEcho, msg(brbv1.Variant_VARIANT_BRACHA, []byte("v"), sum("v"))},
		"digest in a Bracha READY":  {newTestMachine(t), PhaseReady, msg(brbv1.Variant_VARIANT_BRACHA, []byte("v"), sum("v"))},
		"value-only READY (digest)": {digestMachine(t, 4, 1), PhaseReady, msg(digestVariant, []byte("v"), nil)},
	} {
		t.Run(name, func(t *testing.T) {
			// Sent by the origin, so a PROPOSE is not ignored for its sender.
			node := &Replica{self: 1, machine: tc.machine, ctx: t.Context()}
			node.handleFrom(1, tc.phase, tc.msg)
			if len(tc.machine.instances) != 0 {
				t.Fatal("malformed message allocated state")
			}
		})
	}
	// A well-formed digest READY is accepted and counted under the sender.
	m := digestMachine(t, 4, 1)
	node := &Replica{self: 1, machine: m, ctx: t.Context()}
	node.handleFrom(2, PhaseReady, msg(digestVariant, nil, sum("v")))
	if s := m.instances[id]; s == nil || s.readies.counts[string(sum("v"))] != 1 {
		t.Fatal("valid digest READY was not counted")
	}
}

func TestGorumsReadyDigestDelivery(t *testing.T) {
	systems, stop, err := gorums.NewLocalServers(4, gorums.WithLocalDialOptions(gorums.WithGRPCDialOptions(
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	t.Cleanup(cancel)
	nodes := make([]*Replica, 4)
	deliveries := make([]chan Delivery, 4)
	for i, sys := range systems {
		deliveries[i] = make(chan Delivery, 10)
		if nodes[i], err = NewReadyDigestReplica(ctx, sys, 1, func(d Delivery) { deliveries[i] <- d }); err != nil {
			t.Fatal(err)
		}
	}
	serve(t, systems)
	value := bytes.Repeat([]byte("x"), 4096)
	id := BroadcastID{1, 1}
	if err := nodes[0].Broadcast(1, value); err != nil {
		t.Fatal(err)
	}
	for _, ch := range deliveries {
		awaitDelivery(t, ch, id, string(value))
	}
	// A replica can deliver from the other replicas' READYs before its own
	// READY reaches accounting, so wait for every READY call, then freeze the
	// counters. Each READY carries only the digest, so its size does not depend
	// on the 4 KiB value.
	for i, node := range nodes {
		if !waitFor(t, 5*time.Second, func() bool { return node.Traffic(PhaseReady).Calls == 1 }) {
			t.Fatalf("node %d never recorded its READY", i+1)
		}
		node.FinalizeTraffic()
	}
	readySize := uint64(proto.Size(nodes[0].readyDigestMessage(id, sum(string(value)))))
	if readySize >= 64 {
		t.Fatalf("digest READY is %d bytes", readySize)
	}
	for i, node := range nodes {
		got := node.Traffic(PhaseReady)
		if got.Calls != 1 || got.RemoteMessages != 3 || got.RemoteBytes != 3*readySize {
			t.Fatalf("node %d READY traffic %+v, want 3 remote messages of %d bytes", i+1, got, readySize)
		}
	}
}
