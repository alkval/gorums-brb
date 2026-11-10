package bench

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
	"github.com/relab/gorums/benchkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// holdingPeer counts the ECHO handlers that have started and keeps each one
// until hold is closed.
type holdingPeer struct {
	entered *atomic.Int64
	hold    <-chan struct{}
}

func (holdingPeer) Propose(gorums.ServerContext, *brbv1.PhaseMessage) {}
func (p holdingPeer) Echo(gorums.ServerContext, *brbv1.PhaseMessage) {
	p.entered.Add(1)
	<-p.hold
}
func (holdingPeer) Ready(gorums.ServerContext, *brbv1.PhaseMessage) {}
func (holdingPeer) QEcho(gorums.ServerContext, *brbv1.PhaseMessage) {}

// interceptorPair is two servers. Server 1 runs the benchmark interceptor in
// front of a holdingPeer; server 2 only sends.
type interceptorPair struct {
	toFirst  gorums.Config // server 2's view of server 1
	entered  atomic.Int64
	ready    chan struct{}
	cancel   context.CancelFunc     // ends the interceptor's run context
	returned chan error             // result of each BRB request's handler chain
	release  func()                 // lets held handlers return
	send     func(seq uint64) error // one ECHO from server 2 to server 1
}

func newInterceptorPair(t *testing.T) *interceptorPair {
	t.Helper()
	p := &interceptorPair{ready: make(chan struct{}), returned: make(chan error, 64)}
	runCtx, cancel := context.WithCancel(t.Context())
	p.cancel = cancel
	hold := make(chan struct{})
	var once sync.Once
	p.release = func() { once.Do(func() { close(hold) }) }
	observe := func(ctx gorums.ServerContext, msg *gorums.Message, next gorums.Handler) (*gorums.Message, error) {
		out, err := next(ctx, msg)
		if strings.HasPrefix(msg.GetMethod(), "brb.v1.Bracha.") {
			p.returned <- err
		}
		return out, err
	}
	systems, stop, err := gorums.NewLocalServers(2,
		gorums.WithLocalDialOptions(gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))),
		gorums.WithLocalServerOptionsFunc(func(id gorums.ID) []gorums.ServerOption {
			if id != 1 {
				return nil
			}
			return []gorums.ServerOption{gorums.WithServerInterceptors(observe, serverInterceptor(runCtx, p.ready, 2))}
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	t.Cleanup(func() { // before the servers stop
		cancel()
		p.release()
	})
	brbv1.RegisterBrachaServer(systems[0], holdingPeer{&p.entered, hold})
	for _, srv := range systems {
		benchkit.RegisterControlServer(srv, benchkit.NewControl())
		go func() { _ = srv.ListenAndServe() }()
	}
	ctx, done := context.WithTimeout(t.Context(), 5*time.Second)
	defer done()
	for _, srv := range systems {
		if err := srv.WaitForPeers(ctx, func(c gorums.Config) bool { return len(c) == 2 }); err != nil {
			t.Fatal(err)
		}
	}
	p.toFirst = gorums.Config{systems[1].PeerConfig()[0]}
	p.send = func(seq uint64) error {
		msg := brbv1.PhaseMessage_builder{
			Id:    brbv1.BroadcastID_builder{Origin: 2, Sequence: seq}.Build(),
			Value: []byte("v"),
		}.Build()
		return brbv1.Echo(p.toFirst.Context(t.Context()), msg).Send()
	}
	return p
}

func enteredWithin(p *interceptorPair, timeout time.Duration, cond func(int64) bool) bool {
	deadline := time.Now().Add(timeout)
	for !cond(p.entered.Load()) {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
	return true
}

// Once the replica is ready, the interceptor must not release Gorums'
// dispatcher: while one BRB handler runs, later requests on the same stream
// wait in the dispatcher's bounded queue.
func TestInterceptorKeepsDispatcherAfterReady(t *testing.T) {
	const requests = 8
	p := newInterceptorPair(t)
	close(p.ready)
	for seq := uint64(1); seq <= requests; seq++ {
		if err := p.send(seq); err != nil {
			t.Fatal(err)
		}
	}
	if !enteredWithin(p, 5*time.Second, func(n int64) bool { return n >= 1 }) {
		t.Fatal("no BRB handler started")
	}
	if enteredWithin(p, 300*time.Millisecond, func(n int64) bool { return n > 1 }) {
		t.Fatalf("%d BRB handlers started on one stream while the first was running, want 1", p.entered.Load())
	}
	p.release()
	if !enteredWithin(p, 5*time.Second, func(n int64) bool { return n == requests }) {
		t.Fatalf("%d of %d BRB handlers ran after the first returned", p.entered.Load(), requests)
	}
}

// A BRB request that arrives before the replica is ready waits at the
// interceptor without blocking its stream: a startup probe on the same stream
// still completes. The request runs once the replica is ready, or returns the
// run context's error if the run is cancelled first.
func TestInterceptorHoldsEarlyRequests(t *testing.T) {
	for _, end := range []string{"ready", "cancel"} {
		t.Run(end, func(t *testing.T) {
			p := newInterceptorPair(t)
			p.release() // handlers return at once; only the startup gate holds
			if err := p.send(1); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if err := probePeers(ctx, p.toFirst); err != nil {
				t.Fatalf("startup probe behind an early BRB request: %v", err)
			}
			if got := p.entered.Load(); got != 0 {
				t.Fatalf("%d BRB handlers ran before the replica was ready", got)
			}
			select {
			case err := <-p.returned:
				t.Fatalf("early BRB request returned before ready: %v", err)
			default:
			}
			if end == "ready" {
				close(p.ready)
			} else {
				p.cancel()
			}
			select {
			case err := <-p.returned:
				if end == "ready" && (err != nil || p.entered.Load() != 1) {
					t.Fatalf("after ready: err=%v, handlers=%d, want the handler to run once", err, p.entered.Load())
				}
				if end == "cancel" && (!errors.Is(err, context.Canceled) || p.entered.Load() != 0) {
					t.Fatalf("after cancel: err=%v, handlers=%d, want context cancellation and no handler", err, p.entered.Load())
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("early BRB request still waiting after %s", end)
			}
		})
	}
}
