package brb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func localNodes(t *testing.T, count, f int) ([]*GorumsNode, []*gorums.System, []chan Delivery) {
	t.Helper()
	opts := gorums.WithDialOptions(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	systems, stop, err := gorums.NewLocalSystems(count, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	t.Cleanup(cancel)
	nodes := make([]*GorumsNode, count)
	deliveries := make([]chan Delivery, count)
	for i, sys := range systems {
		deliveries[i] = make(chan Delivery, 100)
		nodes[i], err = NewGorumsNode(ctx, sys, f, func(d Delivery) { deliveries[i] <- d })
		if err != nil {
			t.Fatal(err)
		}
	}
	return nodes, systems, deliveries
}

// Establish all connections before testing protocol messages. This prototype
// does not provide transport recovery for unavailable peers.
func serve(t *testing.T, systems []*gorums.System) {
	t.Helper()
	for _, sys := range systems {
		go func() { _ = sys.Serve() }()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, sys := range systems {
		if err := sys.WaitForConfig(ctx, func(c gorums.Configuration) bool {
			return len(c) == len(systems)
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func awaitDelivery(t *testing.T, ch <-chan Delivery, id BroadcastID, value string) {
	t.Helper()
	select {
	case d := <-ch:
		if d.ID != id || string(d.Value) != value {
			t.Fatalf("unexpected delivery: %+v", d)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("delivery timed out")
	}
}

func TestGorumsFourNodesDeliver(t *testing.T) {
	nodes, systems, deliveries := localNodes(t, 4, 1)
	serve(t, systems)

	if err := nodes[0].Broadcast(1, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	for i, ch := range deliveries {
		awaitDelivery(t, ch, BroadcastID{Origin: 1, Sequence: 1}, "hello")
		t.Logf("node %d delivered hello", i+1)
	}
}

func TestGorumsConcurrentBroadcastsAndSequenceReuse(t *testing.T) {
	nodes, systems, deliveries := localNodes(t, 4, 1)
	serve(t, systems)
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if nodes[0].Broadcast(1, []byte("one")) == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted %d uses of one sequence", accepted.Load())
	}
	for _, ch := range deliveries {
		awaitDelivery(t, ch, BroadcastID{1, 1}, "one")
	}
	for i, node := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := node.Broadcast(2, []byte{byte(i)}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for _, ch := range deliveries {
		seen := map[BroadcastID]bool{}
		for range nodes {
			select {
			case d := <-ch:
				if seen[d.ID] || d.ID.Origin < 1 || d.ID.Origin > ProcessID(len(nodes)) || d.ID.Sequence != 2 || len(d.Value) != 1 || int(d.Value[0]) != int(d.ID.Origin)-1 {
					t.Fatalf("invalid delivery %+v", d)
				}
				seen[d.ID] = true
			case <-time.After(8 * time.Second):
				t.Fatal("concurrent broadcast timed out")
			}
		}
	}
}

func TestGorumsRejectsMissingConfiguration(t *testing.T) {
	if _, err := NewGorumsNode(t.Context(), nil, 1, nil); err == nil {
		t.Fatal("accepted nil system")
	}
	sys, err := gorums.NewSystem("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Stop()
	if _, err := NewGorumsNode(nil, sys, 0, nil); err == nil {
		t.Fatal("accepted nil context")
	}
	if _, err := NewGorumsNode(t.Context(), sys, 0, nil); err == nil {
		t.Fatal("accepted absent configuration")
	}
}

func TestBroadcastRejectsZeroSequenceAndCancelledContext(t *testing.T) {
	systems, stop, err := gorums.NewLocalSystems(1,
		gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	node, err := NewGorumsNode(ctx, systems[0], 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Broadcast(0, []byte("value")); err == nil {
		t.Fatal("accepted sequence zero")
	}
	cancel()
	if err := node.Broadcast(1, []byte("value")); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context cancellation", err)
	}
	if len(node.started) != 0 {
		t.Fatal("invalid broadcast reserved a sequence")
	}
}

func TestGorumsDeliversEmptyValue(t *testing.T) {
	nodes, systems, deliveries := localNodes(t, 1, 0)
	serve(t, systems)
	if err := nodes[0].Broadcast(1, nil); err != nil {
		t.Fatal(err)
	}
	awaitDelivery(t, deliveries[0], BroadcastID{1, 1}, "")
}

func TestPhaseMessageOwnsPayload(t *testing.T) {
	value := []byte("original")
	id := BroadcastID{1, 2}
	msg := phaseMessage(3, id, value)
	value[0] = 'X'
	if msg.GetSender() != 3 || msg.GetId().GetOrigin() != 1 || msg.GetId().GetSequence() != 2 || string(msg.GetValue()) != "original" {
		t.Fatal("phase message lost its identifier or shared the input payload")
	}
}
