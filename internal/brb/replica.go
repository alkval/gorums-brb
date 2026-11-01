package brb

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
	"google.golang.org/protobuf/proto"
)

// Delivery is a value delivered by this process. At is the time of the state
// transition that delivered it, which can precede the callback.
type Delivery struct {
	ID    BroadcastID
	Value []byte
	At    time.Time
}

// Traffic counts the protocol messages this replica asked Gorums to send in one
// phase, once per logical multicast. Bytes are marshalled PhaseMessage sizes.
// They exclude the Gorums envelope, gRPC and HTTP/2 framing, and TCP/IP
// headers. Local messages are those this replica handles itself. Counts are
// queued attempts: messages later discarded at Stop or failed are included.
type Traffic struct {
	Calls          uint64 // multicast calls
	RemoteMessages uint64
	RemoteBytes    uint64
	LocalMessages  uint64
	LocalBytes     uint64
}

// ErrQueueCapacityExceeded reports that a dispatch queue could not accept
// work the protocol requires. It latches: the replica stops processing and
// the run is invalid.
var ErrQueueCapacityExceeded = errors.New("brb: outbound peer queue capacity exceeded")

// ErrReplicaStopped is returned by Broadcast after Stop.
var ErrReplicaStopped = errors.New("brb: replica stopped")

// Option configures a Replica.
type Option func(*replicaOptions)

type replicaOptions struct {
	peerEntries, selfEntries int
	peerBytes, selfBytes     int64
	now                      func() time.Time
	// Test hooks, called without dispatchMu unless noted.
	beforePeerSend       func(peer ProcessID, phase Phase, id BroadcastID)
	beforeSelfDeliver    func(Delivery)
	beforeTransitionExit func() // called with dispatchMu held
}

// WithPeerQueueLimits bounds each remote peer's queue. The default is
// (2n+1)·WindowSize entries ((3n+1)·WindowSize for the optimistic variant)
// and 128 MiB.
func WithPeerQueueLimits(entries int, bytes int64) Option {
	return func(o *replicaOptions) { o.peerEntries, o.peerBytes = entries, bytes }
}

// WithSelfQueueLimits bounds the local delivery queue. The defaults match the
// peer queue defaults.
func WithSelfQueueLimits(entries int, bytes int64) Option {
	return func(o *replicaOptions) { o.selfEntries, o.selfBytes = entries, bytes }
}

// DefaultQueueBytes is the default byte bound of each dispatch queue.
const DefaultQueueBytes int64 = 128 << 20

// Replica connects the state machine to Gorums. Votes are counted by the peer
// ID that Gorums reports, which is verified only when the peers are configured
// with TLS identities.
//
// Each remote peer has a bounded queue and one worker that sends to that peer
// alone, so a slow peer delays only its own messages. Transitions and the
// queue insertions they cause run under dispatchMu, which keeps every
// recipient's messages in transition order. If a queue is full, the replica
// latches ErrQueueCapacityExceeded instead of waiting or dropping work.
type Replica struct {
	self      ProcessID
	machine   *StateMachine
	config    gorums.Config
	ctx       context.Context // lifetime of the experiment, not one incoming request
	onDeliver func(Delivery)
	opts      replicaOptions

	dispatchMu     sync.Mutex
	stopped        bool // set by Stop under dispatchMu
	dispatchCtx    context.Context
	dispatchCancel context.CancelFunc
	peers          []*peerQueue // indexed by ID-1; nil for self
	selfQueue      *selfQueue   // nil without onDeliver
	workers        sync.WaitGroup
	stopOnce       sync.Once

	windowExceeded   atomic.Bool
	capacityExceeded atomic.Bool
	capacityErr      error             // written before capacityExceeded is set
	capacity         *CapacitySnapshot // written before capacityExceeded is set
	capacityLatched  chan struct{}

	sendFailures [PhaseQEcho + 1]atomic.Uint64
	failureMu    sync.Mutex
	firstFailure *SendFailureSnapshot

	// One mutex keeps each multicast's counts and every snapshot consistent.
	trafficMu    sync.Mutex
	trafficFinal bool
	traffic      [PhaseQEcho + 1]Traffic // indexed by Phase
	sends        [PhaseQEcho + 1]sendStats
}

// NewReplica registers the adapter before server.Serve and starts its send
// workers. All processes must use the same fixed configuration (IDs 1..n) and
// fault bound. Keep ctx alive throughout the run. To shut down, cancel ctx,
// stop the server, then call Stop. The caller owns ctx and the server.
//
// onDeliver runs on one goroutine, one delivery at a time in local delivery
// order, shortly after the transition that enabled it (Delivery.At). It must
// return promptly and must not call Stop. It may call Broadcast and the
// diagnostic methods.
func NewReplica(ctx context.Context, server *gorums.Server, f int, onDeliver func(Delivery), opts ...Option) (*Replica, error) {
	return newReplica(ctx, server, f, onDeliver, brbv1.Variant_VARIANT_BRACHA, opts)
}

// NewOptimisticReplica uses rotating committees with no fallback or recovery.
// All members must select this variant, and faulty committee members may stall it.
func NewOptimisticReplica(ctx context.Context, server *gorums.Server, f int, onDeliver func(Delivery), opts ...Option) (*Replica, error) {
	return newReplica(ctx, server, f, onDeliver, brbv1.Variant_VARIANT_OPTIMISTIC, opts)
}

// NewReadyDigestReplica runs Bracha with a SHA-256 digest instead of the value
// in READY. All members must select this variant.
func NewReadyDigestReplica(ctx context.Context, server *gorums.Server, f int, onDeliver func(Delivery), opts ...Option) (*Replica, error) {
	return newReplica(ctx, server, f, onDeliver, brbv1.Variant_VARIANT_BRACHA_READY_DIGEST, opts)
}

func newReplica(ctx context.Context, server *gorums.Server, f int, onDeliver func(Delivery), variant brbv1.Variant, opts []Option) (*Replica, error) {
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
	switch variant {
	case brbv1.Variant_VARIANT_OPTIMISTIC:
		machine, err = NewOptimisticStateMachine(len(config), f, self)
	case brbv1.Variant_VARIANT_BRACHA_READY_DIGEST:
		machine, err = NewReadyDigestStateMachine(len(config), f)
	}
	if err != nil {
		return nil, err
	}
	perBroadcast := 2*len(config) + 1 // PROPOSE, ECHO and READY
	if variant == brbv1.Variant_VARIANT_OPTIMISTIC {
		perBroadcast = 3*len(config) + 1 // and Q-ECHO
	}
	o := replicaOptions{
		peerEntries: perBroadcast * int(WindowSize), peerBytes: DefaultQueueBytes,
		selfEntries: perBroadcast * int(WindowSize), selfBytes: DefaultQueueBytes,
		now: time.Now,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.peerEntries < 1 || o.peerBytes < 1 || o.selfEntries < 1 || o.selfBytes < 1 {
		return nil, errors.New("brb: queue limits must be positive")
	}
	node := &Replica{
		self: self, machine: machine, config: config, ctx: ctx,
		onDeliver: onDeliver, opts: o,
		peers:           make([]*peerQueue, len(config)),
		capacityLatched: make(chan struct{}),
	}
	node.dispatchCtx, node.dispatchCancel = context.WithCancel(ctx)
	for i, peer := range config {
		if ProcessID(peer.ID()) == self {
			continue
		}
		q := newPeerQueue(peer, o.peerEntries, o.peerBytes)
		node.peers[i] = q
		node.workers.Go(func() { node.runPeer(q) })
	}
	if onDeliver != nil {
		node.selfQueue = newSelfQueue(o.selfEntries, o.selfBytes)
		node.workers.Go(node.runSelf)
	}
	brbv1.RegisterBrachaServer(server, node)
	return node, nil
}

// Broadcast publishes PROPOSE to every process, including this one, and
// returns once the message is queued for each remote peer and this replica
// has processed its own copy. A nil result means accepted, not sent or
// delivered. A sequence cannot be reused, even after an error, since some
// recipients may already have received the value. Send failures to individual
// peers are reported by SendFailures.
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
	if err := n.CapacityError(); err != nil {
		return err
	}
	id := BroadcastID{Origin: n.self, Sequence: sequence}
	if err := n.machine.reserve(n.dispatchCtx, id); err != nil {
		return n.admissionError(err)
	}
	msg := n.message(id, value)
	n.dispatchMu.Lock()
	defer n.dispatchMu.Unlock()
	if n.stopped {
		return cmp.Or(n.ctx.Err(), ErrReplicaStopped)
	}
	if err := n.CapacityError(); err != nil {
		return err
	}
	// Queue PROPOSE before handling the local copy, so that every recipient
	// receives it ahead of the ECHO it causes here.
	toSelf, ok := n.enqueueLocked(PhasePropose, n.config, msg, brbv1.Propose)
	if ok && toSelf {
		n.transitionLocked(n.self, PhasePropose, msg)
	}
	return n.CapacityError()
}

// admissionError explains a failed reservation: a latched capacity failure or
// a stop cancels the wait, and so does the run context.
func (n *Replica) admissionError(err error) error {
	if capErr := n.CapacityError(); capErr != nil {
		return capErr
	}
	if ctxErr := n.ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if n.dispatchCtx.Err() != nil {
		return ErrReplicaStopped
	}
	return err
}

type multicastFunc func(*brbv1.ConfigContext, *brbv1.PhaseMessage) *gorums.OnewayCall[*brbv1.PhaseMessage]

// enqueueLocked queues msg for every remote recipient and records it as one
// logical multicast. It reports whether the recipients include this replica.
// If any remote queue lacks room, nothing is queued or recorded and the
// capacity latch trips.
// The caller holds dispatchMu.
func (n *Replica) enqueueLocked(phase Phase, recipients gorums.Config, msg *brbv1.PhaseMessage, call multicastFunc) (toSelf, ok bool) {
	size := int64(proto.Size(msg))
	charge := size + slotCharge
	for _, peer := range recipients {
		q := n.peerQueue(ProcessID(peer.ID()))
		if q == nil {
			toSelf = toSelf || ProcessID(peer.ID()) == n.self
			continue
		}
		if fits, c := q.fits(charge); !fits {
			n.tripCapacityLatchLocked(q.id, phase, msgID(msg), charge, c)
			return false, false
		}
	}
	n.recordSendSize(phase, recipients, uint64(size))
	for _, peer := range recipients {
		if q := n.peerQueue(ProcessID(peer.ID())); q != nil {
			q.push(peerItem{phase: phase, id: msgID(msg), msg: msg, call: call, charge: charge})
		}
	}
	return toSelf, true
}

func (n *Replica) peerQueue(id ProcessID) *peerQueue {
	if id == 0 || int(id) > len(n.peers) {
		return nil
	}
	return n.peers[id-1]
}

func msgID(msg *brbv1.PhaseMessage) BroadcastID {
	return BroadcastID{Origin: ProcessID(msg.GetId().GetOrigin()), Sequence: msg.GetId().GetSequence()}
}

// runPeer sends one peer's messages in order until the queue closes or the
// dispatch context ends. A message keeps its credit until Send returns.
func (n *Replica) runPeer(q *peerQueue) {
	for {
		item, ok := q.next(n.dispatchCtx)
		if !ok {
			return
		}
		if hook := n.opts.beforePeerSend; hook != nil {
			hook(q.id, item.phase, item.id)
		}
		token, start := n.sends[item.phase].begin()
		err := item.call(q.config.Context(n.dispatchCtx), item.msg).Send()
		cancelled := n.dispatchCtx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, gorums.ErrStopped)
		n.sends[item.phase].end(token, start, err, cancelled)
		if err != nil && !cancelled {
			n.recordSendFailure(q.id, item.phase, item.id, err)
		}
		q.done(item)
	}
}

// runSelf invokes onDeliver for each queued delivery, without locks, until
// Stop has closed intake and the queue is empty.
func (n *Replica) runSelf() {
	for {
		item, ok := n.selfQueue.next()
		if !ok {
			return
		}
		if hook := n.opts.beforeSelfDeliver; hook != nil {
			hook(item.delivery)
		}
		n.selfQueue.started(n.opts.now().Sub(item.delivery.At))
		n.onDeliver(item.delivery)
		n.selfQueue.done(item)
	}
}

// tripCapacityLatchLocked records the first capacity failure, stops dispatch
// and makes the replica ignore further messages. The caller holds dispatchMu.
func (n *Replica) tripCapacityLatchLocked(peer ProcessID, phase Phase, id BroadcastID, charge int64, c credits) {
	if n.capacityExceeded.Load() {
		return
	}
	n.capacity = &CapacitySnapshot{Peer: peer, Phase: phase, ID: id, MsgBytes: charge, Queue: queueSnapshot(c)}
	n.capacityErr = fmt.Errorf("%w: peer %d phase %s id %d/%d (entries %d+%d of %d, bytes %d+%d of %d, message %d B)",
		ErrQueueCapacityExceeded, peer, phaseLabel(phase), id.Origin, id.Sequence,
		c.queuedEntries, c.inFlightEntries, c.maxEntries, c.queuedBytes, c.inFlightBytes, c.maxBytes, charge)
	n.capacityExceeded.Store(true)
	close(n.capacityLatched)
	log.Printf("brb node %d: %v", n.self, n.capacityErr)
	n.dispatchCancel()
}

// CapacityError reports a latched queue capacity failure. The replica ignores
// all messages after it, so the run is invalid.
func (n *Replica) CapacityError() error {
	if n.capacityExceeded.Load() {
		return n.capacityErr
	}
	return nil
}

// CapacityStatus returns the first capacity failure, or nil.
func (n *Replica) CapacityStatus() *CapacitySnapshot {
	if !n.capacityExceeded.Load() {
		return nil
	}
	s := *n.capacity
	return &s
}

// CapacityLatched is closed when the capacity latch trips.
func (n *Replica) CapacityLatched() <-chan struct{} {
	return n.capacityLatched
}

// PeerQueues reports each remote peer's queue, ordered by ID.
func (n *Replica) PeerQueues() []PeerQueueSnapshot {
	var out []PeerQueueSnapshot
	for _, q := range n.peers {
		if q != nil {
			out = append(out, q.snapshot())
		}
	}
	return out
}

// SelfQueue reports the local delivery queue. It is zero without onDeliver.
func (n *Replica) SelfQueue() SelfQueueSnapshot {
	if n.selfQueue == nil {
		return SelfQueueSnapshot{}
	}
	return n.selfQueue.snapshot()
}

// Stop ends dispatch. It waits for any transition in progress, then makes the
// replica ignore later messages and Broadcast calls, finalizes Traffic,
// discards messages not yet sent, and waits for the workers. Queued
// deliveries still reach onDeliver before Stop returns. Call it after
// cancelling the run and stopping the server, never from onDeliver.
func (n *Replica) Stop() {
	n.stopOnce.Do(func() {
		n.dispatchMu.Lock()
		n.stopped = true
		n.FinalizeTraffic()
		n.dispatchMu.Unlock()
		if n.dispatchCancel != nil {
			n.dispatchCancel()
		}
		for _, q := range n.peers {
			if q != nil {
				q.close()
			}
		}
		if n.selfQueue != nil {
			n.selfQueue.closeIntake()
		}
		n.workers.Wait()
	})
}

// SendTiming reports the duration of the per-peer Gorums Send calls for one
// phase, including calls still waiting (censored) when it is called. Time a
// message spends queued before its Send is not included.
func (n *Replica) SendTiming(phase Phase) SendTiming {
	if phase < PhasePropose || phase > PhaseQEcho {
		return SendTiming{}
	}
	return n.sends[phase].snapshot()
}

// WindowViolations reports the receive-window rejections at this replica and
// the snapshot taken at the first one.
func (n *Replica) WindowViolations() (uint64, *WindowSnapshot) {
	return n.machine.WindowViolations()
}

// recordSend counts one attempted multicast. Computing the size once per call
// keeps the cost independent of the number of recipients.
func (n *Replica) recordSend(phase Phase, recipients gorums.Config, msg *brbv1.PhaseMessage) {
	n.recordSendSize(phase, recipients, uint64(proto.Size(msg)))
}

func (n *Replica) recordSendSize(phase Phase, recipients gorums.Config, size uint64) {
	var local uint64
	for _, peer := range recipients {
		if peer.ID() == uint32(n.self) {
			local++
		}
	}
	remote := uint64(len(recipients)) - local
	n.trafficMu.Lock()
	defer n.trafficMu.Unlock()
	if n.trafficFinal {
		return
	}
	t := &n.traffic[phase]
	t.Calls++
	t.RemoteMessages += remote
	t.RemoteBytes += remote * size
	t.LocalMessages += local
	t.LocalBytes += local * size
}

// FinalizeTraffic ends accounting: multicasts recorded afterwards are not
// counted. Server.Stop does not wait for running handlers, so a handler can
// still reach a send after Stop returns. Replica.Stop calls this; it can also
// be called earlier, after cancelling the run and stopping the server.
// Traffic then returns fixed totals.
func (n *Replica) FinalizeTraffic() {
	n.trafficMu.Lock()
	defer n.trafficMu.Unlock()
	n.trafficFinal = true
}

// Traffic reports attempted sends recorded for one phase.
func (n *Replica) Traffic(phase Phase) Traffic {
	if phase < PhasePropose || phase > PhaseQEcho {
		return Traffic{}
	}
	n.trafficMu.Lock()
	defer n.trafficMu.Unlock()
	return n.traffic[phase]
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
	msg := phaseMessage(id, value)
	msg.SetVariant(n.variant())
	return msg
}

// variant names this replica's protocol on the wire. Replicas reject messages
// from any other variant, including values unknown to this version.
func (n *Replica) variant() brbv1.Variant {
	switch {
	case n.machine.optimistic:
		return brbv1.Variant_VARIANT_OPTIMISTIC
	case n.machine.readyDigest:
		return brbv1.Variant_VARIANT_BRACHA_READY_DIGEST
	}
	return brbv1.Variant_VARIANT_BRACHA
}

// readyDigestMessage builds a READY that carries only the value's digest.
func (n *Replica) readyDigestMessage(id BroadcastID, digest []byte) *brbv1.PhaseMessage {
	msg := phaseMessage(id, nil)
	msg.SetVariant(n.variant())
	msg.SetDigest(append([]byte(nil), digest...))
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

// SendFailures reports the per-peer sends of one phase that failed while the
// run was active. A failed send does not establish whether the correct peers
// can deliver, and it does not invalidate the run.
func (n *Replica) SendFailures(phase Phase) uint64 {
	if phase < PhasePropose || phase > PhaseQEcho {
		return 0
	}
	return n.sendFailures[phase].Load()
}

// FirstSendFailure returns the first failed send, or nil.
func (n *Replica) FirstSendFailure() *SendFailureSnapshot {
	n.failureMu.Lock()
	defer n.failureMu.Unlock()
	if n.firstFailure == nil {
		return nil
	}
	s := *n.firstFailure
	return &s
}

// LogMulticastFailures reports failed sends, not failed deliveries. Call once
// at shutdown after stopping the replica.
func (n *Replica) LogMulticastFailures() {
	echo, ready := n.SendFailures(PhaseEcho), n.SendFailures(PhaseReady)
	if echo != 0 || ready != 0 {
		log.Printf("brb node %d: multicast failures: ECHO=%d READY=%d", n.self, echo, ready)
	}
	if count := n.SendFailures(PhaseQEcho); count != 0 {
		log.Printf("brb node %d: Q-ECHO multicast failures=%d", n.self, count)
	}
	if count := n.SendFailures(PhasePropose); count != 0 {
		log.Printf("brb node %d: PROPOSE multicast failures=%d", n.self, count)
	}
}

// recordSendFailure counts a failed send to one peer and logs the first
// failure of each phase.
func (n *Replica) recordSendFailure(peer ProcessID, phase Phase, id BroadcastID, err error) {
	n.failureMu.Lock()
	if n.firstFailure == nil {
		n.firstFailure = &SendFailureSnapshot{Peer: peer, Phase: phase, ID: id, Err: err.Error()}
	}
	n.failureMu.Unlock()
	if n.sendFailures[phase].Add(1) == 1 {
		log.Printf("brb node %d: first %s multicast failure for %v to peer %d: %v (further failures counted)",
			n.self, phaseLabel(phase), id, peer, err)
	}
}

func phaseLabel(p Phase) string {
	switch p {
	case PhasePropose:
		return "PROPOSE"
	case PhaseEcho:
		return "ECHO"
	case PhaseReady:
		return "READY"
	case PhaseQEcho:
		return "Q-ECHO"
	}
	return fmt.Sprintf("phase-%d", p)
}

func (n *Replica) handle(ctx gorums.ServerContext, phase Phase, msg *brbv1.PhaseMessage) {
	// Release Gorums before waiting for dispatchMu, so that other requests on
	// this stream are not blocked behind the transition.
	ctx.Release()
	n.handleFrom(ProcessID(ctx.SenderID()), phase, msg)
}

// handleFrom counts msg as a vote from sender, the peer ID that Gorums reports
// for the stream. With TLS peer identities, Gorums verifies that ID against the
// peer's certificate before any handler runs. This replica handles its own
// messages directly, under its own ID. Senders outside 1..n, such as anonymous
// clients, are rejected by StateMachine.Handle before any state is allocated.
func (n *Replica) handleFrom(sender ProcessID, phase Phase, msg *brbv1.PhaseMessage) {
	if n.ctx.Err() != nil {
		return
	}
	if msg == nil || msg.GetId() == nil {
		log.Printf("brb node %d: missing broadcast ID", n.self)
		return
	}
	if msg.GetVariant() != n.variant() {
		log.Printf("brb node %d: rejected mismatched protocol variant", n.self)
		return
	}
	// Only READY in the READY-digest variant carries a digest, and then no value.
	digestREADY := n.machine.readyDigest && phase == PhaseReady
	if digestREADY && len(msg.GetValue()) != 0 || !digestREADY && len(msg.GetDigest()) != 0 {
		log.Printf("brb node %d: rejected malformed %v message", n.self, phase)
		return
	}
	n.dispatchMu.Lock()
	defer n.dispatchMu.Unlock()
	if n.stopped || n.capacityExceeded.Load() || n.ctx.Err() != nil {
		return
	}
	n.transitionLocked(sender, phase, msg)
	if hook := n.opts.beforeTransitionExit; hook != nil {
		hook()
	}
}

type selfStep struct {
	phase Phase
	msg   *brbv1.PhaseMessage
}

// transitionLocked applies one message, queues the resulting multicasts and
// deliveries, and then applies this replica's own copies of those multicasts
// in turn. The per-instance flags are monotone, so this ends after a few
// steps. The caller holds dispatchMu.
func (n *Replica) transitionLocked(sender ProcessID, phase Phase, msg *brbv1.PhaseMessage) {
	var buf [4]selfStep
	pending := buf[:0]
	for {
		id := msgID(msg)
		var actions []Action
		var err error
		if n.machine.readyDigest && phase == PhaseReady {
			actions, err = n.machine.HandleReadyDigest(sender, id, msg.GetDigest())
		} else {
			actions, err = n.machine.Handle(sender, phase, id, msg.GetValue())
		}
		if err != nil {
			if errors.Is(err, ErrOutsideWindow) {
				n.windowExceeded.Store(true)
			}
			log.Printf("brb node %d: %v", n.self, err)
		}
		for _, action := range actions {
			pending = n.publishLocked(action, pending)
			if n.capacityExceeded.Load() {
				return
			}
		}
		if len(pending) == 0 {
			return
		}
		sender, phase, msg = n.self, pending[0].phase, pending[0].msg
		pending = pending[1:]
	}
}

// publishLocked queues one action and appends this replica's own copy of a
// multicast to pending. The caller holds dispatchMu.
func (n *Replica) publishLocked(action Action, pending []selfStep) []selfStep {
	var phase Phase
	var recipients gorums.Config
	var msg *brbv1.PhaseMessage
	var call multicastFunc
	switch action.Type {
	case ActionMulticastEcho:
		phase, recipients, msg, call = PhaseEcho, n.recipients(action), n.message(action.ID, action.Value), brbv1.Echo
	case ActionMulticastReady:
		msg = n.message(action.ID, action.Value)
		if action.Digest != nil {
			msg = n.readyDigestMessage(action.ID, action.Digest)
		}
		phase, recipients, call = PhaseReady, n.config, brbv1.Ready
	case ActionMulticastQEcho:
		phase, recipients, msg, call = PhaseQEcho, n.recipients(action), n.message(action.ID, action.Value), brbv1.QEcho
	case ActionDeliver:
		n.deliverLocked(action)
		return pending
	default:
		return pending
	}
	if toSelf, ok := n.enqueueLocked(phase, recipients, msg, call); ok && toSelf {
		pending = append(pending, selfStep{phase, msg})
	}
	return pending
}

// deliverLocked queues a delivery for onDeliver, stamped with the transition
// time. The caller holds dispatchMu.
func (n *Replica) deliverLocked(action Action) {
	if n.onDeliver == nil || n.selfQueue == nil {
		return
	}
	d := Delivery{ID: action.ID, Value: action.Value, At: n.opts.now()}
	charge := int64(len(d.Value)) + slotCharge
	if ok, c := n.selfQueue.push(d, charge); !ok {
		n.tripCapacityLatchLocked(n.self, PhaseReady, action.ID, charge, c)
	}
}

func phaseMessage(id BroadcastID, value []byte) *brbv1.PhaseMessage {
	return brbv1.PhaseMessage_builder{
		Id:    brbv1.BroadcastID_builder{Origin: uint32(id.Origin), Sequence: id.Sequence}.Build(),
		Value: append([]byte(nil), value...),
	}.Build()
}
