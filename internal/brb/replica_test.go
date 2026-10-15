package brb

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMulticastFailureLogging(t *testing.T) {
	// These tests are not parallel: the standard logger is shared.
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	node := &Replica{self: 1}
	node.LogMulticastFailures()
	if output.Len() != 0 {
		t.Fatal("logged failures for a healthy replica")
	}
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			node.recordMulticastFailure(Action{Type: ActionMulticastEcho}, errors.New("echo peer unavailable"))
			node.recordMulticastFailure(Action{Type: ActionMulticastReady}, errors.New("ready peer unavailable"))
		})
	}
	wg.Wait()
	node.LogMulticastFailures()
	got := output.String()
	if strings.Count(got, "\n") != 3 ||
		!strings.Contains(got, "first ECHO multicast failure") ||
		!strings.Contains(got, "echo peer unavailable") ||
		!strings.Contains(got, "first READY multicast failure") ||
		!strings.Contains(got, "ready peer unavailable") ||
		!strings.Contains(got, "multicast failures: ECHO=100 READY=100") {
		t.Fatalf("unexpected failure logs:\n%s", got)
	}
}

func localNodes(t *testing.T, count, f int, localOpts ...gorums.LocalServerOption) ([]*Replica, []*gorums.Server, []chan Delivery) {
	t.Helper()
	opts := gorums.WithGRPCDialOptions(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	localOpts = append([]gorums.LocalServerOption{gorums.WithLocalDialOptions(opts)}, localOpts...)
	systems, stop, err := gorums.NewLocalServers(count, localOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	t.Cleanup(cancel)
	nodes := make([]*Replica, count)
	deliveries := make([]chan Delivery, count)
	for i, sys := range systems {
		deliveries[i] = make(chan Delivery, 100)
		nodes[i], err = NewReplica(ctx, sys, f, func(d Delivery) { deliveries[i] <- d })
		if err != nil {
			t.Fatal(err)
		}
	}
	return nodes, systems, deliveries
}

// Establish connections between the servers participating in this test.
func serve(t *testing.T, systems []*gorums.Server) {
	t.Helper()
	for _, sys := range systems {
		go func() { _ = sys.ListenAndServe() }()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, sys := range systems {
		if err := sys.WaitForPeers(ctx, func(c gorums.Config) bool {
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

// This peer receives all three phases but never sends any BRB messages.
type silentBroadcast struct{}

func (silentBroadcast) Propose(gorums.ServerContext, *brbv1.PhaseMessage) {}
func (silentBroadcast) Echo(gorums.ServerContext, *brbv1.PhaseMessage)    {}
func (silentBroadcast) Ready(gorums.ServerContext, *brbv1.PhaseMessage)   {}
func (silentBroadcast) QEcho(gorums.ServerContext, *brbv1.PhaseMessage)   {}

func TestGorumsFourNodesWithSilentPeer(t *testing.T) {
	const lateValue = "late conflicting value"
	// Observe when injected messages finish processing so the duplicate check
	// does not depend on a sleep or on multicast returning before its handlers.
	handled := make(chan struct{}, 12)
	observe := gorums.WithServerInterceptors(func(ctx gorums.ServerContext, msg *gorums.Message, next gorums.Handler) (*gorums.Message, error) {
		out, err := next(ctx, msg)
		if phase, ok := msg.Proto.(*brbv1.PhaseMessage); ok && string(phase.GetValue()) == lateValue {
			handled <- struct{}{}
		}
		return out, err
	})
	nodes, systems, deliveries := localNodes(t, 4, 1, gorums.WithLocalServerOptions(observe))
	// Replace node 4's BRB handlers before starting any servers. Its Gorums
	// connections remain active, but it contributes no ECHO or READY votes.
	brbv1.RegisterBrachaServer(systems[3], silentBroadcast{})
	serve(t, systems)

	id := BroadcastID{Origin: 1, Sequence: 1}
	if err := nodes[0].Broadcast(id.Sequence, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		awaitDelivery(t, deliveries[i], id, "hello")
		t.Logf("node %d delivered hello without node 4's votes", i+1)
	}

	// Re-send each phase for the delivered ID, even with a different value.
	// These calls go through Gorums rather than directly into the state machine.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cc := systems[0].PeerConfig().Context(ctx)
	msg := phaseMessage(1, id, []byte(lateValue))
	if err := brbv1.Propose(cc, msg).Send(); err != nil {
		t.Fatal(err)
	}
	if err := brbv1.Echo(cc, msg).Send(); err != nil {
		t.Fatal(err)
	}
	if err := brbv1.Ready(cc, msg).Send(); err != nil {
		t.Fatal(err)
	}
	for range 12 { // three phases handled at each of the four servers
		select {
		case <-handled:
		case <-ctx.Done():
			t.Fatal("late messages were not handled before the deadline")
		}
	}
	for i, ch := range deliveries {
		select {
		case d := <-ch:
			t.Fatalf("node %d produced an extra delivery: %+v", i+1, d)
		default:
		}
	}
}

func TestGorumsDeliversWithUnavailablePeer(t *testing.T) {
	for _, crash := range []bool{false, true} {
		name := "never-starts"
		if crash {
			name = "crashes-after-delivery"
		}
		t.Run(name, func(t *testing.T) {
			// Correct-peer connections are established before this diagnostic
			// uses fail-fast sends to the unavailable fourth peer.
			nodes, systems, deliveries := localNodes(t, 4, 1, gorums.WithLocalDialOptions(
				gorums.WithGRPCDialOptions(grpc.WithDefaultCallOptions(grpc.WaitForReady(false)))))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for _, node := range nodes {
				node.ctx = ctx // set before starting handlers
			}
			sequence := uint64(1)
			if crash {
				serve(t, systems)
				if err := nodes[0].Broadcast(sequence, []byte("before crash")); err != nil {
					t.Fatal(err)
				}
				for _, ch := range deliveries {
					awaitDelivery(t, ch, BroadcastID{1, sequence}, "before crash")
				}
				sequence++
			}
			systems[3].Stop()
			if !crash {
				serve(t, systems[:3])
			}
			for _, sys := range systems[:3] {
				if err := sys.WaitForPeers(ctx, func(c gorums.Config) bool { return len(c) == 3 }); err != nil {
					t.Fatal(err)
				}
			}
			sent := make(chan error, 1)
			go func() { sent <- nodes[0].Broadcast(sequence, []byte("survivors")) }()
			for i, ch := range deliveries[:3] {
				awaitDelivery(t, ch, BroadcastID{1, sequence}, "survivors")
				t.Logf("node %d delivered with node 4 unavailable", i+1)
			}
			select {
			case err := <-sent:
				if !errors.Is(err, gorums.ErrSendFailure) {
					t.Fatalf("got %v, want a reported send failure for the unavailable peer", err)
				}
			case <-time.After(time.Second):
				t.Fatal("all survivors delivered, but Broadcast is still waiting for node 4")
			}
		})
	}
}

func TestGorumsConcurrentBroadcastsAndSequenceReuse(t *testing.T) {
	nodes, systems, deliveries := localNodes(t, 4, 1)
	serve(t, systems)
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for range 10 {
		wg.Go(func() {
			if nodes[0].Broadcast(1, []byte("one")) == nil {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted %d uses of one sequence", accepted.Load())
	}
	for _, ch := range deliveries {
		awaitDelivery(t, ch, BroadcastID{1, 1}, "one")
	}
	for i, node := range nodes {
		wg.Go(func() {
			if err := node.Broadcast(2, []byte{byte(i)}); err != nil {
				t.Error(err)
			}
		})
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
	if _, err := NewReplica(t.Context(), nil, 1, nil); err == nil {
		t.Fatal("accepted nil server")
	}
	sys := gorums.NewServer()
	defer sys.Stop()
	//nolint:staticcheck // Deliberately exercise rejection of a nil context.
	if _, err := NewReplica(nil, sys, 0, nil); err == nil {
		t.Fatal("accepted nil context")
	}
	if _, err := NewReplica(t.Context(), sys, 0, nil); err == nil {
		t.Fatal("accepted absent configuration")
	}
}

func TestBroadcastRejectsZeroSequenceAndCancelledContext(t *testing.T) {
	systems, stop, err := gorums.NewLocalServers(1,
		gorums.WithLocalDialOptions(gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	node, err := NewReplica(ctx, systems[0], 0, nil)
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
	if len(node.machine.instances) != 0 {
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

func TestGorumsFailedReadyMulticastStillDelivers(t *testing.T) {
	nodes, _, deliveries := localNodes(t, 2, 0)
	node := nodes[0]
	id := BroadcastID{Origin: 2, Sequence: 1}
	value := []byte("hello")
	actions, err := node.machine.Handle(2, PhaseReady, id, value)
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != 2 || actions[0].Type != ActionMulticastReady || actions[1].Type != ActionDeliver {
		t.Fatalf("expected READY and delivery actions, got %+v", actions)
	}

	// Cancel after the transition has enabled delivery, forcing the outbound
	// READY to fail without needing a network timeout or a mock transport.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	node.ctx = ctx
	if err := brbv1.Ready(node.config.Context(ctx), phaseMessage(node.self, id, value)).Send(); err == nil {
		t.Fatal("expected multicast to fail with a cancelled context")
	}
	node.performActions(actions)
	if got := node.readyFailures.Load(); got != 1 {
		t.Fatalf("READY failure count = %d, want 1", got)
	}
	node.performActions([]Action{{Type: ActionMulticastEcho, ID: id, Value: value}})
	if got := node.echoFailures.Load(); got != 1 {
		t.Fatalf("ECHO failure count = %d, want 1", got)
	}
	select {
	case delivery := <-deliveries[0]:
		if delivery.ID != id || string(delivery.Value) != string(value) {
			t.Fatalf("unexpected delivery: %+v", delivery)
		}
	default:
		t.Fatal("failed READY multicast suppressed local delivery")
	}

	// The delivered instance must still ignore a duplicate READY.
	if actions, err := node.machine.Handle(2, PhaseReady, id, value); err != nil || len(actions) != 0 {
		t.Fatalf("duplicate READY returned actions=%+v err=%v", actions, err)
	}
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

func TestWindowErrorPersistsAfterSuccessfulDelivery(t *testing.T) {
	handled := make(chan struct{}, 1)
	observe := gorums.WithServerInterceptors(func(ctx gorums.ServerContext, msg *gorums.Message, next gorums.Handler) (*gorums.Message, error) {
		out, err := next(ctx, msg)
		if phase, ok := msg.Proto.(*brbv1.PhaseMessage); ok && phase.GetId().GetSequence() == ^uint64(0) {
			handled <- struct{}{}
		}
		return out, err
	})
	nodes, systems, deliveries := localNodes(t, 1, 0, gorums.WithLocalServerOptions(observe))
	serve(t, systems)
	if err := nodes[0].WindowError(); err != nil {
		t.Fatalf("fresh replica: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := brbv1.Echo(systems[0].PeerConfig().Context(ctx), phaseMessage(1, BroadcastID{1, ^uint64(0)}, []byte("future"))).Send(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatal("future message was not handled")
	}
	if err := nodes[0].Broadcast(1, []byte("valid")); err != nil {
		t.Fatal(err)
	}
	awaitDelivery(t, deliveries[0], BroadcastID{1, 1}, "valid")
	if !errors.Is(nodes[0].WindowError(), ErrOutsideWindow) {
		t.Fatal("successful delivery cleared the window error")
	}
}
