package brb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relab/gorums"
	"github.com/relab/gorums/benchkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Test-only options for the hooks in replicaOptions.
func withClock(now func() time.Time) Option { return func(o *replicaOptions) { o.now = now } }
func withBeforePeerSend(hook func(ProcessID, Phase, BroadcastID)) Option {
	return func(o *replicaOptions) { o.beforePeerSend = hook }
}
func withBeforeSelfDeliver(hook func(Delivery)) Option {
	return func(o *replicaOptions) { o.beforeSelfDeliver = hook }
}
func withBeforeTransitionExit(hook func()) Option {
	return func(o *replicaOptions) { o.beforeTransitionExit = hook }
}

type replicaConstructor func(context.Context, *gorums.Server, int, func(Delivery), ...Option) (*Replica, error)

// testCluster creates count replicas. optsFor returns each replica's options
// (by ID). The servers are not serving until serve is called.
func testCluster(t *testing.T, count, f int, ctor replicaConstructor, onDeliver func(ProcessID, Delivery),
	optsFor func(ProcessID) []Option, serverOpts ...gorums.LocalServerOption) ([]*Replica, []*gorums.Server) {
	t.Helper()
	dial := gorums.WithGRPCDialOptions(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	serverOpts = append([]gorums.LocalServerOption{gorums.WithLocalDialOptions(dial)}, serverOpts...)
	systems, stop, err := gorums.NewLocalServers(count, serverOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	nodes := make([]*Replica, count)
	for i, sys := range systems {
		id := ProcessID(i + 1)
		var deliver func(Delivery)
		if onDeliver != nil {
			deliver = func(d Delivery) { onDeliver(id, d) }
		}
		var opts []Option
		if optsFor != nil {
			opts = optsFor(id)
		}
		nodes[i], err = ctor(ctx, sys, f, deliver, opts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nodes[i].Stop)
	}
	return nodes, systems
}

// contractLog collects deliveries per replica.
type contractLog struct {
	mu   sync.Mutex
	got  map[ProcessID][]Delivery
	note chan struct{}
}

func newContractLog() *contractLog {
	return &contractLog{got: make(map[ProcessID][]Delivery), note: make(chan struct{}, 1)}
}

func (l *contractLog) add(id ProcessID, d Delivery) {
	l.mu.Lock()
	l.got[id] = append(l.got[id], d)
	l.mu.Unlock()
	select {
	case l.note <- struct{}{}:
	default:
	}
}

func (l *contractLog) of(id ProcessID) []Delivery {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.got[id])
}

func (l *contractLog) has(id ProcessID, want BroadcastID) bool {
	return slices.ContainsFunc(l.of(id), func(d Delivery) bool { return d.ID == want })
}

// gate blocks callers until it is opened, and counts the callers that entered.
type gate struct {
	open    chan struct{}
	once    sync.Once
	entered atomic.Int32
}

func newGate() *gate { return &gate{open: make(chan struct{})} }

func (g *gate) wait() {
	g.entered.Add(1)
	<-g.open
}

func (g *gate) release() { g.once.Do(func() { close(g.open) }) }

func TestContract1BOriginalProposePublishedAndDeliveredAllVariants(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ctor  replicaConstructor
		n, f  int
		count uint64
	}{
		{"bracha", NewReplica, 4, 1, 4},
		{"ready-digest", NewReadyDigestReplica, 4, 1, 4},
		{"optimistic", NewOptimisticReplica, 4, 1, 4}, // rotates every committee role
		{"single", NewReplica, 1, 0, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := newContractLog()
			nodes, systems := testCluster(t, tc.n, tc.f, tc.ctor, log.add, nil)
			serve(t, systems)
			for seq := uint64(1); seq <= tc.count; seq++ {
				if err := nodes[0].Broadcast(seq, fmt.Appendf(nil, "v%d", seq)); err != nil {
					t.Fatal(err)
				}
			}
			for i := range nodes {
				id := ProcessID(i + 1)
				for seq := uint64(1); seq <= tc.count; seq++ {
					want := BroadcastID{1, seq}
					if !waitFor(t, 8*time.Second, func() bool { return log.has(id, want) }) {
						t.Fatalf("node %d did not deliver %v", id, want)
					}
				}
			}
			p := nodes[0].Traffic(PhasePropose)
			if p.Calls != tc.count || p.RemoteMessages != uint64(tc.n-1)*tc.count || p.LocalMessages != tc.count {
				t.Fatalf("origin PROPOSE traffic = %+v", p)
			}
		})
	}
}

func TestContract1BOptimisticOriginOutsideReceivingCommittee(t *testing.T) {
	log := newContractLog()
	nodes, systems := testCluster(t, 4, 1, NewOptimisticReplica, log.add, nil)
	serve(t, systems)
	// With n=4 and f=1 the PROPOSE committee has 3 members; sequence 2 of
	// origin 1 starts the committee at node 2, leaving node 1 out.
	id := BroadcastID{1, 2}
	if nodes[0].machine.accepts(1, PhasePropose, id) {
		t.Fatal("test setup: origin is in the PROPOSE committee")
	}
	if err := nodes[0].machine.reserve(t.Context(), BroadcastID{1, 1}); err != nil {
		t.Fatal(err) // skip sequence 1 without broadcasting it
	}
	if err := nodes[0].Broadcast(id.Sequence, []byte("v")); err != nil {
		t.Fatal(err)
	}
	for _, peer := range []ProcessID{2, 3, 4} {
		if !waitFor(t, 8*time.Second, func() bool { return log.has(peer, id) }) {
			t.Fatalf("committee member %d did not deliver", peer)
		}
	}
	if !waitFor(t, 8*time.Second, func() bool { return log.has(1, id) }) {
		t.Fatal("origin did not deliver")
	}
	if p := nodes[0].Traffic(PhasePropose); p.Calls != 1 || p.RemoteMessages != 3 || p.LocalMessages != 1 {
		t.Fatalf("PROPOSE traffic = %+v, want all four members", p)
	}
	if e := nodes[0].Traffic(PhaseEcho); e.Calls != 0 {
		t.Fatalf("origin outside the committee sent ECHO: %+v", e)
	}
}

func TestContract1BCallerValueSliceMutationSafe(t *testing.T) {
	log := newContractLog()
	nodes, systems := testCluster(t, 4, 1, NewReplica, log.add, nil)
	serve(t, systems)
	value := []byte("original")
	if err := nodes[0].Broadcast(1, value); err != nil {
		t.Fatal(err)
	}
	copy(value, "XXXXXXXX")
	for i := range nodes {
		id := ProcessID(i + 1)
		if !waitFor(t, 8*time.Second, func() bool { return log.has(id, BroadcastID{1, 1}) }) {
			t.Fatalf("node %d did not deliver", id)
		}
		if got := string(log.of(id)[0].Value); got != "original" {
			t.Fatalf("node %d delivered %q", id, got)
		}
	}
}

// The servers are not serving and the only remote send is held, so the queue
// to node 2 fills deterministically.
func TestContract1BSequenceConsumedOnProposeCapacityFailure(t *testing.T) {
	held := newGate()
	defer held.release()
	nodes, _ := testCluster(t, 2, 0, NewReplica, nil, func(id ProcessID) []Option {
		return []Option{WithPeerQueueLimits(1, DefaultQueueBytes), withBeforePeerSend(func(ProcessID, Phase, BroadcastID) { held.wait() })}
	})
	node := nodes[0]
	// PROPOSE takes the only slot; the local ECHO it causes does not fit.
	if err := node.Broadcast(1, []byte("a")); !errors.Is(err, ErrQueueCapacityExceeded) {
		t.Fatalf("got %v, want capacity failure", err)
	}
	status := node.CapacityStatus()
	if status == nil || status.Peer != 2 || status.Phase != PhaseEcho || status.ID != (BroadcastID{1, 1}) || status.Queue.MaxEntries != 1 {
		t.Fatalf("capacity status = %+v", status)
	}
	if err := node.Broadcast(1, []byte("b")); !errors.Is(err, ErrQueueCapacityExceeded) {
		t.Fatalf("retry: got %v, want the latched capacity failure", err)
	}
	if err := node.machine.reserve(t.Context(), BroadcastID{1, 1}); err == nil || err.Error() != "brb: sequence already used" {
		t.Fatalf("sequence 1 is reusable after the failure: %v", err)
	}
	select {
	case <-node.CapacityLatched():
	default:
		t.Fatal("capacity latch channel not closed")
	}
}

// blockingWriter holds every write until released.
type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	buf     bytes.Buffer
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// The latch cancels dispatch and releases dispatchMu before the failure is
// logged, so a blocked log writer delays neither the cancellation nor Stop.
func TestContract1BCapacityLatchDoesNotWaitForLogger(t *testing.T) {
	held := newGate()
	defer held.release()
	nodes, _ := testCluster(t, 2, 0, NewReplica, nil, func(ProcessID) []Option {
		return []Option{WithPeerQueueLimits(1, DefaultQueueBytes), withBeforePeerSend(func(ProcessID, Phase, BroadcastID) { held.wait() })}
	})
	node := nodes[0]
	w := &blockingWriter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	previous := log.Writer()
	log.SetOutput(w)
	var once sync.Once
	unblock := func() { once.Do(func() { close(w.release) }) }
	t.Cleanup(func() { unblock(); log.SetOutput(previous) })
	result := make(chan error, 1)
	go func() { result <- node.Broadcast(1, []byte("v")) }()
	select {
	case <-node.CapacityLatched():
	case <-time.After(5 * time.Second):
		t.Fatal("latch did not trip")
	}
	select {
	case <-node.dispatchCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch not cancelled while the logger is blocked")
	}
	select {
	case <-w.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("capacity failure was never logged")
	}
	held.release()
	stopped := make(chan struct{})
	go func() { node.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop waited for the blocked logger")
	}
	unblock()
	if err := <-result; !errors.Is(err, ErrQueueCapacityExceeded) {
		t.Fatalf("Broadcast: %v", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if got := strings.Count(w.buf.String(), "outbound peer queue capacity exceeded"); got != 1 {
		t.Fatalf("logged %d capacity failures:\n%s", got, w.buf.String())
	}
	if s := node.CapacityStatus(); s == nil || s.At.IsZero() {
		t.Fatalf("capacity status without a latch time: %+v", s)
	}
}

func TestContract1BProposeOrderedBeforeEchoInRecipientQueue(t *testing.T) {
	var mu sync.Mutex
	order := map[ProcessID][]Phase{}
	log := newContractLog()
	nodes, systems := testCluster(t, 4, 1, NewReplica, log.add, func(id ProcessID) []Option {
		if id != 1 {
			return nil
		}
		return []Option{withBeforePeerSend(func(peer ProcessID, phase Phase, bid BroadcastID) {
			if bid == (BroadcastID{1, 1}) {
				mu.Lock()
				order[peer] = append(order[peer], phase)
				mu.Unlock()
			}
		})}
	})
	serve(t, systems)
	if err := nodes[0].Broadcast(1, []byte("v")); err != nil {
		t.Fatal(err)
	}
	for _, peer := range []ProcessID{2, 3, 4} {
		if !waitFor(t, 8*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(order[peer]) == 3
		}) {
			t.Fatalf("node 1 did not send three phases to node %d", peer)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for peer, phases := range order {
		if !slices.Equal(phases, []Phase{PhasePropose, PhaseEcho, PhaseReady}) {
			t.Fatalf("node 1 sent %v to node %d, want PROPOSE, ECHO, READY", phases, peer)
		}
	}
}

// A single replica delivers each broadcast during Broadcast itself, so the
// delivery queue is filled without any network.
func TestContract1BSelfQueueBoundsHeldCallback(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries int
		bytes   int64
	}{
		{"entries", 3, DefaultQueueBytes},
		{"bytes", 100, 3 * (8 + slotCharge)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			held := newGate()
			defer held.release()
			var mu sync.Mutex
			var got []uint64
			nodes, _ := testCluster(t, 1, 0, NewReplica, func(_ ProcessID, d Delivery) {
				if d.ID.Sequence == 1 {
					held.wait()
				}
				mu.Lock()
				got = append(got, d.ID.Sequence)
				mu.Unlock()
			}, func(ProcessID) []Option { return []Option{WithSelfQueueLimits(tc.entries, tc.bytes)} })
			node := nodes[0]
			value := []byte("12345678")
			if err := node.Broadcast(1, value); err != nil {
				t.Fatal(err)
			}
			if !waitFor(t, 5*time.Second, func() bool { return held.entered.Load() == 1 }) {
				t.Fatal("callback did not start")
			}
			for seq := uint64(2); seq <= 3; seq++ {
				if err := node.Broadcast(seq, value); err != nil {
					t.Fatal(err)
				}
			}
			q := node.SelfQueue().Queue
			charge := int64(len(value) + slotCharge)
			if q.QueuedEntries != 2 || q.InFlightEntries != 1 || q.QueuedBytes != 2*charge || q.InFlightBytes != charge {
				t.Fatalf("self queue = %+v", q)
			}
			if err := node.Broadcast(4, value); !errors.Is(err, ErrQueueCapacityExceeded) {
				t.Fatalf("got %v, want capacity failure", err)
			}
			if s := node.CapacityStatus(); s == nil || s.Peer != 1 || s.ID != (BroadcastID{1, 4}) {
				t.Fatalf("capacity status = %+v", s)
			}
			held.release()
			if !waitFor(t, 5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 3 }) {
				t.Fatal("queued callbacks did not run")
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(got, []uint64{1, 2, 3}) {
				t.Fatalf("callbacks ran in order %v", got)
			}
		})
	}
}

// Stop runs the queued callbacks before returning, in delivery order, but
// discards messages not yet sent to peers.
func TestContract1BStopDrainsQueuedCallbacksInOrder(t *testing.T) {
	const count = 5
	callback, send := newGate(), newGate()
	defer callback.release()
	defer send.release()
	var mu sync.Mutex
	var got []uint64
	nodes, _ := testCluster(t, 2, 0, NewReplica, func(id ProcessID, d Delivery) {
		if id != 1 {
			return
		}
		if d.ID.Sequence == 1 {
			callback.wait()
		}
		mu.Lock()
		got = append(got, d.ID.Sequence)
		mu.Unlock()
	}, func(ProcessID) []Option {
		return []Option{withBeforePeerSend(func(ProcessID, Phase, BroadcastID) { send.wait() })}
	})
	node := nodes[0]
	for seq := uint64(1); seq <= count; seq++ {
		if err := node.Broadcast(seq, []byte("v")); err != nil {
			t.Fatal(err)
		}
		// Node 2's ECHO completes the quorum, then node 1's own READY delivers.
		node.handleFrom(2, PhaseEcho, node.message(BroadcastID{1, seq}, []byte("v")))
	}
	if q := node.SelfQueue().Queue; q.QueuedEntries+q.InFlightEntries != count {
		t.Fatalf("self queue = %+v, want %d deliveries", q, count)
	}
	if !waitFor(t, 5*time.Second, func() bool { return send.entered.Load() == 1 }) {
		t.Fatal("peer worker did not take its first message")
	}
	stopped := make(chan struct{})
	go func() { node.Stop(); close(stopped) }()
	if !waitFor(t, 5*time.Second, func() bool { return node.PeerQueues()[0].DiscardedAtStopEntries > 0 }) {
		t.Fatal("Stop did not discard the unsent peer messages")
	}
	send.release()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a callback was still running")
	case <-time.After(50 * time.Millisecond):
	}
	callback.release()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(got, []uint64{1, 2, 3, 4, 5}) {
		t.Fatalf("callbacks before Stop returned: %v", got)
	}
	if q := node.SelfQueue().Queue; q.QueuedEntries != 0 || q.InFlightEntries != 0 || q.HighWaterEntries != count {
		t.Fatalf("self queue after Stop = %+v", q)
	}
	p := node.PeerQueues()[0]
	// PROPOSE, ECHO and READY per broadcast; the first was in flight.
	if p.DiscardedAtStopEntries != 3*count-1 || p.Queue.QueuedEntries != 0 {
		t.Fatalf("peer queue after Stop = %+v", p)
	}
	if err := node.Broadcast(count+1, []byte("v")); !errors.Is(err, ErrReplicaStopped) {
		t.Fatalf("Broadcast after Stop: %v", err)
	}
}

func TestContract1BDeliveryTimestampIsTransitionTime(t *testing.T) {
	const delay = 50 * time.Millisecond
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	transition := now()
	got := make(chan Delivery, 1)
	nodes, _ := testCluster(t, 1, 0, NewReplica, func(_ ProcessID, d Delivery) { got <- d }, func(ProcessID) []Option {
		return []Option{withClock(now), withBeforeSelfDeliver(func(Delivery) { clock.Add(int64(delay)) })}
	})
	if err := nodes[0].Broadcast(1, []byte("v")); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if !d.At.Equal(transition) {
			t.Fatalf("Delivery.At = %v, want the transition time %v", d.At, transition)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery")
	}
	if s := nodes[0].SelfQueue(); s.MaxCallbackLag < delay || s.Queue.HighWaterEntries != 1 {
		t.Fatalf("self queue = %+v, want lag >= %v", s, delay)
	}
}

func TestContract1BCallbackReentrancyNoDeadlock(t *testing.T) {
	const count = 20
	var node *Replica
	var ready sync.WaitGroup
	ready.Add(1)
	done := make(chan struct{})
	var seen atomic.Uint64
	nodes, _ := testCluster(t, 1, 0, NewReplica, func(_ ProcessID, d Delivery) {
		ready.Wait()
		_ = node.Traffic(PhasePropose)
		_ = node.SendTiming(PhaseEcho)
		_, _ = node.WindowViolations()
		_ = node.SelfQueue()
		_ = node.PeerQueues()
		if err := node.CapacityError(); err != nil {
			panic(err)
		}
		if seen.Add(1) == count {
			close(done)
			return
		}
		if err := node.Broadcast(d.ID.Sequence+1, d.Value); err != nil {
			panic(err)
		}
	}, nil)
	node = nodes[0]
	ready.Done()
	if err := node.Broadcast(1, []byte("v")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("reentrant broadcasts stalled after %d deliveries", seen.Load())
	}
	node.Stop()
	if got := node.Traffic(PhasePropose).Calls; got != count {
		t.Fatalf("PROPOSE calls = %d, want %d", got, count)
	}
}

// A handler that trips the latch before Stop acquires dispatchMu keeps its
// error; a handler that arrives after Stop changes nothing.
func TestContract1BStopBarrier(t *testing.T) {
	t.Run("capacity failure before Stop", func(t *testing.T) {
		send, exit := newGate(), newGate()
		defer send.release()
		defer exit.release()
		var holdExit atomic.Bool
		nodes, _ := testCluster(t, 2, 0, NewReplica, nil, func(ProcessID) []Option {
			return []Option{
				WithPeerQueueLimits(2, DefaultQueueBytes),
				withBeforePeerSend(func(ProcessID, Phase, BroadcastID) { send.wait() }),
				withBeforeTransitionExit(func() {
					if holdExit.Load() {
						exit.wait()
					}
				}),
			}
		})
		node := nodes[0]
		if err := node.Broadcast(1, []byte("v")); err != nil { // PROPOSE in flight, ECHO queued
			t.Fatal(err)
		}
		holdExit.Store(true)
		handled := make(chan struct{})
		go func() {
			// The quorum enables READY, which does not fit.
			node.handleFrom(2, PhaseEcho, node.message(BroadcastID{1, 1}, []byte("v")))
			close(handled)
		}()
		if !waitFor(t, 5*time.Second, func() bool { return exit.entered.Load() == 1 }) {
			t.Fatal("handler did not reach the end of its transition")
		}
		stopped := make(chan struct{})
		go func() { node.Stop(); close(stopped) }()
		select {
		case <-stopped:
			t.Fatal("Stop did not wait for the transition in progress")
		case <-time.After(50 * time.Millisecond):
		}
		exit.release()
		send.release()
		<-handled
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not return")
		}
		if err := node.CapacityError(); !errors.Is(err, ErrQueueCapacityExceeded) {
			t.Fatalf("capacity error after Stop = %v", err)
		}
	})
	t.Run("handler after Stop", func(t *testing.T) {
		nodes, _ := testCluster(t, 2, 0, NewReplica, nil, func(ProcessID) []Option {
			return []Option{WithPeerQueueLimits(1, DefaultQueueBytes)}
		})
		node := nodes[0]
		node.Stop()
		node.handleFrom(2, PhasePropose, node.message(BroadcastID{2, 1}, []byte("v")))
		if err := node.CapacityError(); err != nil {
			t.Fatalf("closed queues produced a capacity error: %v", err)
		}
		if got := node.Traffic(PhaseEcho); got != (Traffic{}) {
			t.Fatalf("handler after Stop sent %+v", got)
		}
		if len(node.machine.instances) != 0 {
			t.Fatal("handler after Stop changed state")
		}
	})
}

func TestContract1BTwoWayCallsSucceedUnderHeavyRelayTraffic(t *testing.T) {
	log := newContractLog()
	nodes, systems := testCluster(t, 4, 1, NewReplica, log.add, nil)
	for _, sys := range systems {
		benchkit.RegisterControlServer(sys, benchkit.NewControl())
	}
	serve(t, systems)
	const perNode = 200
	value := bytes.Repeat([]byte{'x'}, 4096)
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Go(func() {
			for seq := uint64(1); seq <= perNode; seq++ {
				if err := node.Broadcast(seq, value); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	calls := 0
	for calls < 50 || len(log.of(1)) < 4*perNode {
		if _, err := benchkit.ClockSync(systems[0].PeerConfig().Context(ctx), benchkit.ClockSyncRequest_builder{}.Build()).All(); err != nil {
			t.Fatalf("ClockSync %d failed during relay traffic: %v", calls, err)
		}
		calls++
	}
	wg.Wait()
	for _, node := range nodes {
		if err := node.CapacityError(); err != nil {
			t.Fatal(err)
		}
	}
}

// Node 2 handles node 1's ECHOs, but each causes no further messages.
func TestContract1BInFlightCreditHeldUntilSendReturns(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(fmt.Sprintf("release=%t", release), func(t *testing.T) {
			first := newGate()
			defer first.release()
			var sends atomic.Int32
			nodes, systems := testCluster(t, 2, 0, NewReplica, nil, func(id ProcessID) []Option {
				return []Option{WithPeerQueueLimits(2, DefaultQueueBytes), withBeforePeerSend(func(ProcessID, Phase, BroadcastID) {
					if sends.Add(1) == 1 {
						first.wait()
					}
				})}
			})
			serve(t, systems)
			node := nodes[0]
			echo := func(seq uint64) { // PROPOSE from origin 2 makes node 1 send one ECHO
				node.handleFrom(2, PhasePropose, node.message(BroadcastID{2, seq}, []byte("v")))
			}
			echo(1)
			if !waitFor(t, 5*time.Second, func() bool { return first.entered.Load() == 1 }) {
				t.Fatal("worker did not take the first message")
			}
			if q := node.PeerQueues()[0].Queue; q.InFlightEntries != 1 || q.QueuedEntries != 0 {
				t.Fatalf("after taking item 1: %+v", q)
			}
			echo(2)
			if q := node.PeerQueues()[0].Queue; q.InFlightEntries != 1 || q.QueuedEntries != 1 {
				t.Fatalf("after queueing item 2: %+v", q)
			}
			if release {
				first.release()
				if !waitFor(t, 5*time.Second, func() bool {
					q := node.PeerQueues()[0].Queue
					return q.InFlightEntries+q.QueuedEntries == 0
				}) {
					t.Fatal("credits were not released after the sends returned")
				}
			}
			echo(3)
			err := node.CapacityError()
			if release && err != nil {
				t.Fatalf("released credit still counted: %v", err)
			}
			if !release && !errors.Is(err, ErrQueueCapacityExceeded) {
				t.Fatalf("in-flight credit not counted: %v", err)
			}
		})
	}
}

func TestContract1BByteBoundAndMaxPayloadExhaustion(t *testing.T) {
	big := bytes.Repeat([]byte{'x'}, MaxPayloadSize)
	limit := int64(3 * MaxPayloadSize / 2)
	t.Run("broadcast", func(t *testing.T) {
		held := newGate()
		defer held.release()
		nodes, _ := testCluster(t, 2, 0, NewReplica, nil, func(ProcessID) []Option {
			return []Option{WithPeerQueueLimits(1000, limit), withBeforePeerSend(func(ProcessID, Phase, BroadcastID) { held.wait() })}
		})
		if err := nodes[0].Broadcast(1, big); !errors.Is(err, ErrQueueCapacityExceeded) {
			t.Fatalf("got %v, want capacity failure", err)
		}
		if s := nodes[0].CapacityStatus(); s == nil || s.Queue.QueuedEntries+s.Queue.InFlightEntries != 1 || s.MsgBytes <= MaxPayloadSize {
			t.Fatalf("capacity status = %+v", s)
		}
	})
	t.Run("relay", func(t *testing.T) {
		held := newGate()
		defer held.release()
		nodes, _ := testCluster(t, 2, 0, NewReplica, nil, func(ProcessID) []Option {
			return []Option{WithPeerQueueLimits(1000, limit), withBeforePeerSend(func(ProcessID, Phase, BroadcastID) { held.wait() })}
		})
		node := nodes[0]
		node.handleFrom(2, PhasePropose, node.message(BroadcastID{2, 1}, big))
		if err := node.CapacityError(); err != nil {
			t.Fatal(err)
		}
		node.handleFrom(2, PhasePropose, node.message(BroadcastID{2, 2}, big))
		if err := node.CapacityError(); !errors.Is(err, ErrQueueCapacityExceeded) {
			t.Fatalf("got %v, want capacity failure", err)
		}
	})
}

func TestContract1BConcurrentBoundaryAdmissionsRace(t *testing.T) {
	const limit = 40
	held := newGate()
	defer held.release()
	nodes, _ := testCluster(t, 4, 1, NewReplica, nil, func(ProcessID) []Option {
		return []Option{WithPeerQueueLimits(limit, DefaultQueueBytes), withBeforePeerSend(func(ProcessID, Phase, BroadcastID) { held.wait() })}
	})
	node := nodes[0]
	var wg sync.WaitGroup
	var next atomic.Uint64
	for w := range 8 {
		wg.Go(func() {
			for range 50 {
				if w%2 == 0 {
					_ = node.Broadcast(next.Add(1), []byte("v"))
					continue
				}
				seq := next.Add(1)
				node.handleFrom(ProcessID(2+w%3), PhasePropose, node.message(BroadcastID{ProcessID(2 + w%3), seq}, []byte("v")))
			}
		})
	}
	wg.Wait()
	if !errors.Is(node.CapacityError(), ErrQueueCapacityExceeded) {
		t.Fatal("tight bound never latched")
	}
	for _, p := range node.PeerQueues() {
		if active := p.Queue.QueuedEntries + p.Queue.InFlightEntries; active > limit || p.Queue.HighWaterEntries > limit {
			t.Fatalf("peer %d exceeded its bound: %+v", p.Peer, p.Queue)
		}
	}
	// Every queued message counts once per remote recipient of a logical
	// multicast, so the queue totals match the recorded traffic.
	var queued, remote uint64
	for _, p := range node.PeerQueues() {
		queued += uint64(p.Queue.QueuedEntries + p.Queue.InFlightEntries)
	}
	for _, phase := range []Phase{PhasePropose, PhaseEcho, PhaseReady} {
		remote += node.Traffic(phase).RemoteMessages
	}
	if queued != remote {
		t.Fatalf("queued %d messages, recorded %d remote messages", queued, remote)
	}
}

// Every replica holds its sends to node 4. Nodes 1-3 deliver without it, and
// node 4 catches up once its links resume, within capacity and the window.
func TestContract1BSlowCorrectPeerRecoversWithinCapacity(t *testing.T) {
	const count = 20
	paused := newGate()
	defer paused.release()
	log := newContractLog()
	nodes, systems := testCluster(t, 4, 1, NewReplica, log.add, func(ProcessID) []Option {
		return []Option{withBeforePeerSend(func(peer ProcessID, _ Phase, _ BroadcastID) {
			if peer == 4 {
				paused.wait()
			}
		})}
	})
	serve(t, systems)
	for seq := uint64(1); seq <= count; seq++ {
		start := time.Now()
		if err := nodes[0].Broadcast(seq, []byte("v")); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > time.Second {
			t.Fatalf("Broadcast %d took %v with node 4 paused", seq, d)
		}
	}
	for _, id := range []ProcessID{1, 2, 3} {
		if !waitFor(t, 8*time.Second, func() bool { return len(log.of(id)) == count }) {
			t.Fatalf("node %d delivered %d of %d with node 4 paused", id, len(log.of(id)), count)
		}
	}
	if got := len(log.of(4)); got != 0 {
		t.Fatalf("node 4 delivered %d while paused", got)
	}
	paused.release()
	if !waitFor(t, 8*time.Second, func() bool { return len(log.of(4)) == count }) {
		t.Fatalf("node 4 delivered %d of %d after resuming", len(log.of(4)), count)
	}
	for i, node := range nodes {
		if err := errors.Join(node.CapacityError(), node.WindowError()); err != nil {
			t.Fatalf("node %d: %v", i+1, err)
		}
	}
}

// More than f stalled links do not block Broadcast; the run fails at the
// first full queue instead. Only node 1's sends to nodes 3 and 4 stall.
func TestContract1BMoreThanFStalledPeersFailsAtCapacity(t *testing.T) {
	const limit = 30
	paused := newGate()
	defer paused.release()
	nodes, systems := testCluster(t, 4, 1, NewReplica, nil, func(id ProcessID) []Option {
		if id != 1 {
			return nil
		}
		return []Option{WithPeerQueueLimits(limit, DefaultQueueBytes), withBeforePeerSend(func(peer ProcessID, _ Phase, _ BroadcastID) {
			if peer >= 3 {
				paused.wait()
			}
		})}
	})
	serve(t, systems)
	var err error
	seq := uint64(0)
	for err == nil && seq < limit {
		seq++
		start := time.Now()
		err = nodes[0].Broadcast(seq, []byte("v"))
		if d := time.Since(start); d > time.Second {
			t.Fatalf("Broadcast %d blocked for %v", seq, d)
		}
		// Let the queue to the healthy peer drain, so only the stalled ones fill.
		waitFor(t, time.Second, func() bool { q := nodes[0].PeerQueues()[0].Queue; return q.QueuedEntries+q.InFlightEntries == 0 })
	}
	if !waitFor(t, 8*time.Second, func() bool { return nodes[0].CapacityError() != nil }) {
		t.Fatalf("no capacity failure after %d broadcasts (last error %v)", seq, err)
	}
	if s := nodes[0].CapacityStatus(); s == nil || s.Peer < 3 {
		t.Fatalf("capacity status = %+v, want a stalled peer", s)
	}
}

// Traffic counts each logical multicast once; SendTiming counts the sends to
// each remote peer.
func TestContract1BTrafficAndPerPeerSendTiming(t *testing.T) {
	log := newContractLog()
	nodes, systems := testCluster(t, 4, 1, NewReplica, log.add, nil)
	serve(t, systems)
	id := BroadcastID{1, 1}
	if err := nodes[0].Broadcast(1, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 8*time.Second, func() bool { return nodes[0].SendTiming(PhasePropose).Succeeded == 3 }) {
		t.Fatalf("PROPOSE sends = %+v, want three", nodes[0].SendTiming(PhasePropose))
	}
	size := uint64(proto.Size(nodes[0].message(id, []byte("v"))))
	want := Traffic{Calls: 1, RemoteMessages: 3, RemoteBytes: 3 * size, LocalMessages: 1, LocalBytes: size}
	if got := nodes[0].Traffic(PhasePropose); got != want {
		t.Fatalf("PROPOSE traffic = %+v, want %+v", got, want)
	}
}

// BenchmarkBroadcastLocalDelivery measures closed-loop broadcasts through
// local delivery at the origin, over loopback with 4 KiB values, with one
// worker and with eight concurrent workers at the origin. It uses only the
// public API, so the same code runs against the previous implementation.
func BenchmarkBroadcastLocalDelivery(b *testing.B) {
	for _, n := range []int{4, 13} {
		for _, workers := range []int{1, 8} {
			b.Run(fmt.Sprintf("n=%d/workers=%d", n, workers), func(b *testing.B) {
				benchmarkBroadcast(b, n, workers)
			})
		}
	}
}

func benchmarkBroadcast(b *testing.B, n, workers int) {
	dial := gorums.WithGRPCDialOptions(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	systems, stop, err := gorums.NewLocalServers(n, gorums.WithLocalDialOptions(dial))
	if err != nil {
		b.Fatal(err)
	}
	defer stop()
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()
	var mu sync.Mutex
	waiters := map[uint64]chan struct{}{}
	nodes := make([]*Replica, n)
	for i, sys := range systems {
		var deliver func(Delivery)
		if i == 0 {
			deliver = func(d Delivery) {
				if d.ID.Origin != 1 {
					return
				}
				mu.Lock()
				ch := waiters[d.ID.Sequence]
				delete(waiters, d.ID.Sequence)
				mu.Unlock()
				if ch != nil {
					close(ch)
				}
			}
		}
		if nodes[i], err = NewReplica(ctx, sys, (n-1)/3, deliver); err != nil {
			b.Fatal(err)
		}
	}
	for _, sys := range systems {
		go func() { _ = sys.ListenAndServe() }()
	}
	for _, sys := range systems {
		if err := sys.WaitForPeers(ctx, func(c gorums.Config) bool { return len(c) == n }); err != nil {
			b.Fatal(err)
		}
	}
	value := bytes.Repeat([]byte{'x'}, 4096)
	var next atomic.Uint64
	b.ResetTimer()
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				seq := next.Add(1)
				if seq > uint64(b.N) {
					return
				}
				ch := make(chan struct{})
				mu.Lock()
				waiters[seq] = ch
				mu.Unlock()
				if err := nodes[0].Broadcast(seq, value); err != nil {
					b.Error(err)
					return
				}
				<-ch
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	cancel()
	for _, sys := range systems {
		sys.Stop()
	}
	stopAll(nodes)
}

func stopAll(nodes []*Replica) {
	for _, node := range nodes {
		node.Stop()
	}
}
