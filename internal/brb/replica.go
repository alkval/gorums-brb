package brb

import (
	"context"
	"errors"
	"log"
	"sync/atomic"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
	"google.golang.org/protobuf/proto"
)

// Delivery is a value delivered by this process.
type Delivery struct {
	ID    BroadcastID
	Value []byte
}

// Traffic counts the protocol messages this replica asked Gorums to send in one
// phase. Bytes are marshalled PhaseMessage sizes. They exclude the Gorums
// envelope, gRPC and HTTP/2 framing, and TCP/IP headers. Local messages are
// sends to this replica itself, which the pinned Gorums runtime handles
// in-process. Counts are attempts: failed sends are also counted here.
type Traffic struct {
	Calls          uint64 // multicast calls
	RemoteMessages uint64
	RemoteBytes    uint64
	LocalMessages  uint64
	LocalBytes     uint64
}

type trafficCounter struct {
	calls, remoteMessages, remoteBytes, localMessages, localBytes atomic.Uint64
}

// Replica connects the state machine to Gorums multicast.
// Sender IDs are trusted for now. Authentication is deliberately deferred.
type Replica struct {
	self           ProcessID
	machine        *StateMachine
	config         gorums.Config
	ctx            context.Context // lifetime of the experiment, not one incoming request
	onDeliver      func(Delivery)
	windowExceeded atomic.Bool
	echoFailures   atomic.Uint64
	readyFailures  atomic.Uint64
	qEchoFailures  atomic.Uint64
	traffic        [PhaseQEcho + 1]trafficCounter // indexed by Phase
}

// NewReplica registers the adapter before server.Serve. All processes must
// use the same fixed configuration (IDs 1..n) and fault bound. Keep ctx alive
// throughout the run, then cancel it before server.Stop. The caller owns both.
// onDeliver must be quick and safe for concurrent calls.
func NewReplica(ctx context.Context, server *gorums.Server, f int, onDeliver func(Delivery)) (*Replica, error) {
	return newReplica(ctx, server, f, onDeliver, false)
}

// NewOptimisticReplica uses rotating committees with no fallback or recovery.
// All members must select this variant, and faulty committee members may stall it.
func NewOptimisticReplica(ctx context.Context, server *gorums.Server, f int, onDeliver func(Delivery)) (*Replica, error) {
	return newReplica(ctx, server, f, onDeliver, true)
}

func newReplica(ctx context.Context, server *gorums.Server, f int, onDeliver func(Delivery), optimistic bool) (*Replica, error) {
	if ctx == nil || server == nil {
		return nil, errors.New("brb: nil run context or Gorums server")
	}
	config := server.PeerConfig()
	machine, err := NewStateMachine(len(config), f)
	if err != nil {
		return nil, err
	}
	for i, peer := range config {
		if peer == nil || peer.ID() != uint32(i+1) {
			return nil, errors.New("brb: configuration must contain sorted IDs 1..n")
		}
	}
	self := ProcessID(server.NodeID())
	if self == 0 || uint64(self) > uint64(len(config)) {
		return nil, errors.New("brb: server identity is outside configuration")
	}
	if optimistic {
		machine, err = NewOptimisticStateMachine(len(config), f, self)
		if err != nil {
			return nil, err
		}
	}
	node := &Replica{
		self: self, machine: machine, config: config, ctx: ctx,
		onDeliver: onDeliver,
	}
	brbv1.RegisterBrachaServer(server, node)
	return node, nil
}

// Broadcast sends PROPOSE to every process, including this one. A nil result
// means sent, not delivered. A sequence cannot be reused, even after a send
// error, since some recipients may already have received the value. A send
// failure alone does not establish whether the correct peers can deliver.
func (n *Replica) Broadcast(sequence uint64, value []byte) error {
	if sequence == 0 {
		return errors.New("brb: sequence must be positive")
	}
	if len(value) > MaxPayloadSize {
		return errors.New("brb: payload exceeds 1 MiB")
	}
	if err := n.ctx.Err(); err != nil {
		return err
	}
	id := BroadcastID{Origin: n.self, Sequence: sequence}
	if err := n.machine.reserve(n.ctx, id); err != nil {
		return err
	}
	return n.multicast(PhasePropose, n.config, n.message(id, value), brbv1.Propose)
}

type multicastFunc func(*brbv1.ConfigContext, *brbv1.PhaseMessage) *gorums.OnewayCall[*brbv1.PhaseMessage]

func (n *Replica) multicast(phase Phase, recipients gorums.Config, msg *brbv1.PhaseMessage, call multicastFunc) error {
	n.recordSend(phase, recipients, msg)
	return call(recipients.Context(n.ctx), msg).Send()
}

// recordSend counts one attempted multicast before it is sent. Computing the
// size once per call keeps the cost independent of the number of recipients.
func (n *Replica) recordSend(phase Phase, recipients gorums.Config, msg *brbv1.PhaseMessage) {
	size := uint64(proto.Size(msg))
	var local uint64
	for _, peer := range recipients {
		if peer.ID() == uint32(n.self) {
			local++
		}
	}
	remote := uint64(len(recipients)) - local
	c := &n.traffic[phase]
	c.calls.Add(1)
	c.remoteMessages.Add(remote)
	c.remoteBytes.Add(remote * size)
	c.localMessages.Add(local)
	c.localBytes.Add(local * size)
}

// Traffic reports attempted sends for one phase. Read it after cancelling the
// run and stopping the server to obtain final totals.
func (n *Replica) Traffic(phase Phase) Traffic {
	if phase < PhasePropose || phase > PhaseQEcho {
		return Traffic{}
	}
	c := &n.traffic[phase]
	return Traffic{
		Calls:          c.calls.Load(),
		RemoteMessages: c.remoteMessages.Load(),
		RemoteBytes:    c.remoteBytes.Load(),
		LocalMessages:  c.localMessages.Load(),
		LocalBytes:     c.localBytes.Load(),
	}
}

func (n *Replica) Propose(ctx gorums.ServerContext, msg *brbv1.PhaseMessage) {
	n.handle(ctx, PhasePropose, msg)
}

func (n *Replica) Echo(ctx gorums.ServerContext, msg *brbv1.PhaseMessage) {
	n.handle(ctx, PhaseEcho, msg)
}

func (n *Replica) Ready(ctx gorums.ServerContext, msg *brbv1.PhaseMessage) {
	n.handle(ctx, PhaseReady, msg)
}

func (n *Replica) QEcho(ctx gorums.ServerContext, msg *brbv1.PhaseMessage) {
	n.handle(ctx, PhaseQEcho, msg)
}

func (n *Replica) message(id BroadcastID, value []byte) *brbv1.PhaseMessage {
	msg := phaseMessage(n.self, id, value)
	msg.SetOptimistic(n.machine.optimistic)
	return msg
}

// Subsets reuse the existing Gorums connections. PROPOSE and READY reach all.
func (n *Replica) recipients(action Action) gorums.Config {
	if !n.machine.optimistic || action.Type == ActionMulticastReady {
		return n.config
	}
	size := n.machine.f + 1
	if action.Type == ActionMulticastQEcho {
		size = 2*n.machine.f + 1
	}
	peers := make(gorums.Config, 0, size)
	for _, peer := range n.config {
		if n.machine.committeeContains(action.ID, ProcessID(peer.ID()), size) {
			peers = append(peers, peer)
		}
	}
	return peers
}

// WindowError reports a receive-window violation for this replica's lifetime.
// Rejected messages do not stop the protocol, but invalidate benchmark results.
func (n *Replica) WindowError() error {
	if n.windowExceeded.Load() {
		return ErrOutsideWindow
	}
	return nil
}

// LogMulticastFailures reports failed ECHO and READY sends, not failed deliveries.
// Call once at shutdown after cancelling the run and stopping the server.
func (n *Replica) LogMulticastFailures() {
	echo, ready := n.echoFailures.Load(), n.readyFailures.Load()
	if echo != 0 || ready != 0 {
		log.Printf("brb node %d: multicast failures: ECHO=%d READY=%d", n.self, echo, ready)
	}
	if count := n.qEchoFailures.Load(); count != 0 {
		log.Printf("brb node %d: Q-ECHO multicast failures=%d", n.self, count)
	}
}

func (n *Replica) recordMulticastFailure(action Action, err error) {
	count, phase := &n.echoFailures, "ECHO"
	if action.Type == ActionMulticastReady {
		count, phase = &n.readyFailures, "READY"
	}
	if action.Type == ActionMulticastQEcho {
		count, phase = &n.qEchoFailures, "Q-ECHO"
	}
	if count.Add(1) == 1 {
		log.Printf("brb node %d: first %s multicast failure for %v: %v (further failures counted)", n.self, phase, action.ID, err)
	}
}

func (n *Replica) handle(ctx gorums.ServerContext, phase Phase, msg *brbv1.PhaseMessage) {
	// StateMachine.Handle has its own lock. Release Gorums before sending to self
	// or calling user code, which can cause more requests to this node.
	ctx.Release()
	if n.ctx.Err() != nil {
		return
	}
	if msg == nil || msg.GetId() == nil {
		log.Printf("brb node %d: missing broadcast ID", n.self)
		return
	}
	if msg.GetOptimistic() != n.machine.optimistic {
		log.Printf("brb node %d: rejected mismatched protocol variant", n.self)
		return
	}
	id := BroadcastID{Origin: ProcessID(msg.GetId().GetOrigin()), Sequence: msg.GetId().GetSequence()}
	actions, err := n.machine.Handle(ProcessID(msg.GetSender()), phase, id, msg.GetValue())
	if err != nil {
		if errors.Is(err, ErrOutsideWindow) {
			n.windowExceeded.Store(true)
		}
		log.Printf("brb node %d: %v", n.self, err)
		return
	}
	n.performActions(actions)
}

func (n *Replica) performActions(actions []Action) {
	for _, action := range actions {
		var err error
		switch action.Type {
		case ActionMulticastEcho:
			err = n.multicast(PhaseEcho, n.recipients(action), n.message(action.ID, action.Value), brbv1.Echo)
		case ActionMulticastReady:
			err = n.multicast(PhaseReady, n.config, n.message(action.ID, action.Value), brbv1.Ready)
		case ActionMulticastQEcho:
			err = n.multicast(PhaseQEcho, n.recipients(action), n.message(action.ID, action.Value), brbv1.QEcho)
		case ActionDeliver:
			if n.onDeliver != nil {
				n.onDeliver(Delivery{ID: action.ID, Value: action.Value})
			}
		}
		if err != nil {
			// A failed multicast must not skip an already enabled delivery.
			n.recordMulticastFailure(action, err)
		}
	}
}

func phaseMessage(sender ProcessID, id BroadcastID, value []byte) *brbv1.PhaseMessage {
	return brbv1.PhaseMessage_builder{
		Id:    brbv1.BroadcastID_builder{Origin: uint32(id.Origin), Sequence: id.Sequence}.Build(),
		Value: append([]byte(nil), value...), Sender: uint32(sender),
	}.Build()
}
