package brb

import (
	"context"
	"errors"
	"log"
	"sync"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
)

// Delivery is a value delivered by this process.
type Delivery struct {
	ID    BroadcastID
	Value []byte
}

// Replica connects the state machine to Gorums multicast.
// Sender IDs are trusted for now. Authentication is deliberately deferred.
type Replica struct {
	self      ProcessID
	machine   *StateMachine
	config    gorums.Config
	ctx       context.Context // lifetime of the experiment, not one incoming request
	onDeliver func(Delivery)
	mu        sync.Mutex // protects sequence reservations
	started   map[uint64]bool
}

// NewReplica registers the adapter before server.Serve. All processes must
// use the same fixed configuration (IDs 1..n) and fault bound. Keep ctx alive
// throughout the run, then cancel it before server.Stop. The caller owns both.
// onDeliver must be quick and safe for concurrent calls.
func NewReplica(ctx context.Context, server *gorums.Server, f int, onDeliver func(Delivery)) (*Replica, error) {
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
	node := &Replica{
		self: self, machine: machine, config: config, ctx: ctx,
		onDeliver: onDeliver, started: make(map[uint64]bool),
	}
	brbv1.RegisterBrachaServer(server, node)
	return node, nil
}

// Broadcast sends SEND to every process, including this one. A nil result
// means sent, not delivered. A sequence cannot be reused, even after a send
// error, since some recipients may already have received the value.
func (n *Replica) Broadcast(sequence uint64, value []byte) error {
	if sequence == 0 {
		return errors.New("brb: sequence must be positive")
	}
	if err := n.ctx.Err(); err != nil {
		return err
	}
	n.mu.Lock()
	if n.started[sequence] {
		n.mu.Unlock()
		return errors.New("brb: sequence already used")
	}
	n.started[sequence] = true
	n.mu.Unlock()
	id := BroadcastID{Origin: n.self, Sequence: sequence}
	return brbv1.Propose(n.config.Context(n.ctx), phaseMessage(n.self, id, value)).Send()
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
	id := BroadcastID{Origin: ProcessID(msg.GetId().GetOrigin()), Sequence: msg.GetId().GetSequence()}
	actions, err := n.machine.Handle(ProcessID(msg.GetSender()), phase, id, msg.GetValue())
	if err != nil {
		log.Printf("brb node %d: %v", n.self, err)
		return
	}
	for _, action := range actions {
		switch action.Type {
		case ActionMulticastEcho:
			err = brbv1.Echo(n.config.Context(n.ctx), phaseMessage(n.self, action.ID, action.Value)).Send()
		case ActionMulticastReady:
			err = brbv1.Ready(n.config.Context(n.ctx), phaseMessage(n.self, action.ID, action.Value)).Send()
		case ActionDeliver:
			if n.onDeliver != nil {
				n.onDeliver(Delivery{ID: action.ID, Value: action.Value})
			}
		}
		if err != nil {
			// Log the failed multicast. This prototype cannot recover it and
			// does not stop the whole process automatically.
			log.Printf("brb node %d: multicast for %v failed: %v", n.self, id, err)
			return
		}
	}
}

func phaseMessage(sender ProcessID, id BroadcastID, value []byte) *brbv1.PhaseMessage {
	return brbv1.PhaseMessage_builder{
		Id:    brbv1.BroadcastID_builder{Origin: uint32(id.Origin), Sequence: id.Sequence}.Build(),
		Value: append([]byte(nil), value...), Sender: uint32(sender),
	}.Build()
}
