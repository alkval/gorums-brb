package brb

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
)

// countBRBRequests returns a server option for replica id that counts the BRB
// requests whose handler chain has started there.
func countBRBRequests(id ProcessID, entered *atomic.Int64) gorums.LocalServerOption {
	return gorums.WithLocalServerOptionsFunc(func(server gorums.ID) []gorums.ServerOption {
		if ProcessID(server) != id {
			return nil
		}
		return []gorums.ServerOption{gorums.WithServerInterceptors(
			func(ctx gorums.ServerContext, msg *gorums.Message, next gorums.Handler) (*gorums.Message, error) {
				if strings.HasPrefix(msg.GetMethod(), "brb.v1.Bracha.") {
					entered.Add(1)
				}
				return next(ctx, msg)
			})}
	})
}

// transitionHold blocks the first transition that finishes after arm, with
// dispatchMu held, until release.
type transitionHold struct {
	armed   atomic.Bool
	taken   atomic.Bool
	reached chan struct{}
	hold    chan struct{}
	once    sync.Once
	exits   atomic.Int64
}

func newTransitionHold() *transitionHold {
	return &transitionHold{reached: make(chan struct{}), hold: make(chan struct{})}
}

func (h *transitionHold) hook() {
	h.exits.Add(1)
	if h.armed.Load() && h.taken.CompareAndSwap(false, true) {
		close(h.reached)
		<-h.hold
	}
}

func (h *transitionHold) release() { h.once.Do(func() { close(h.hold) }) }

func eventually(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
	return true
}

// While one transition is held, later requests on the same inbound stream
// must not start: the handler keeps Gorums' dispatcher, so they wait in its
// bounded queue. After a short hold they all run.
func TestHeldTransitionAdmitsOneRequestPerStream(t *testing.T) {
	const requests = 16
	var entered atomic.Int64
	hold := newTransitionHold()
	nodes, systems := testCluster(t, 4, 1, NewReplica, nil, func(id ProcessID) []Option {
		if id == 1 {
			return []Option{withBeforeTransitionExit(hold.hook)}
		}
		return nil
	}, countBRBRequests(1, &entered))
	t.Cleanup(hold.release) // before the replicas stop
	serve(t, systems)

	hold.armed.Store(true)
	toFirst := gorums.Config{systems[1].PeerConfig()[0]}
	for seq := uint64(1); seq <= requests; seq++ {
		msg := nodes[1].message(BroadcastID{Origin: 2, Sequence: seq}, []byte("v"))
		if err := brbv1.Echo(toFirst.Context(t.Context()), msg).Send(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-hold.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("no transition reached the hold")
	}
	if eventually(300*time.Millisecond, func() bool { return entered.Load() > 1 }) {
		t.Fatalf("%d requests started on one stream while a transition was held, want 1", entered.Load())
	}

	hold.release()
	if !eventually(5*time.Second, func() bool { return entered.Load() == requests && hold.exits.Load() == requests }) {
		t.Fatalf("after release: %d requests started and %d transitions finished, want %d", entered.Load(), hold.exits.Load(), requests)
	}
	if err := nodes[0].CapacityError(); err != nil {
		t.Fatal(err)
	}
}

// A replica that stays held stops reading from its peers. Their small queues
// toward it fill, and a correct peer latches ErrQueueCapacityExceeded naming
// the held replica. The held replica starts at most one request per peer, and
// everything stops promptly once it is released.
func TestSustainedHoldLatchesPeerCapacity(t *testing.T) {
	const n = 4
	var entered atomic.Int64
	hold := newTransitionHold()
	delivered := make(chan uint64, 1024)
	nodes, systems := testCluster(t, n, 1, NewReplica, func(id ProcessID, d Delivery) {
		if id == 2 && d.ID.Origin == 2 {
			delivered <- d.ID.Sequence
		}
	}, func(id ProcessID) []Option {
		opts := []Option{WithPeerQueueLimits(32, DefaultQueueBytes)}
		if id == 1 {
			opts = append(opts, withBeforeTransitionExit(hold.hook))
		}
		return opts
	}, countBRBRequests(1, &entered), gorums.WithLocalServerOptions(gorums.WithBufferSizes(4, 4)))
	t.Cleanup(hold.release)
	serve(t, systems)

	latched := func() *Replica {
		for _, node := range nodes[1:] {
			if node.CapacityError() != nil {
				return node
			}
		}
		return nil
	}
	hold.armed.Store(true)
	value := bytes.Repeat([]byte{'x'}, 16<<10)
	deadline := time.After(12 * time.Second)
broadcasts:
	for seq := uint64(1); latched() == nil; seq++ {
		if err := nodes[1].Broadcast(seq, value); err != nil {
			if !errors.Is(err, ErrQueueCapacityExceeded) {
				t.Fatalf("broadcast %d: %v", seq, err)
			}
			break
		}
		// Wait for the origin's own delivery, so that it never runs at the
		// edge of its admission window.
		for {
			select {
			case got := <-delivered:
				if got == seq {
					continue broadcasts
				}
			case <-time.After(20 * time.Millisecond):
				if latched() != nil {
					break broadcasts
				}
			case <-deadline:
				t.Fatalf("no peer reached its queue capacity after %d broadcasts", seq)
			}
		}
	}
	node := latched()
	if node == nil {
		t.Fatal("no peer latched a capacity failure")
	}
	if err := node.CapacityError(); !errors.Is(err, ErrQueueCapacityExceeded) {
		t.Fatalf("capacity error = %v", err)
	}
	if status := node.CapacityStatus(); status == nil || status.Peer != 1 {
		t.Fatalf("capacity status = %+v, want the held replica 1", status)
	}
	if got := entered.Load(); got < 1 || got > n-1 {
		t.Fatalf("held replica started %d requests, want one per peer stream at most (%d)", got, n-1)
	}
	if nodes[0].CapacityError() != nil {
		t.Fatalf("held replica latched: %v", nodes[0].CapacityError())
	}

	hold.release()
	stopped := make(chan struct{})
	go func() {
		for _, node := range nodes {
			node.Stop()
		}
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("replicas did not stop after the hold was released")
	}
}
