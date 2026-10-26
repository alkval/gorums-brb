package bench

import (
	"context"
	"fmt"
	"time"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
)

// Faults keep their own identity, because receivers take the sender from the
// transport rather than from the message.
// Their messages carry the run's variant, so correct replicas do not discard
// them as mismatched. They send only full-value PROPOSE and ECHO messages.
type faultyReplica struct {
	ctx     context.Context
	server  *gorums.Server
	mode    string
	variant brbv1.Variant
}

func registerFault(ctx context.Context, server *gorums.Server, mode string, variant brbv1.Variant) {
	brbv1.RegisterBrachaServer(server, &faultyReplica{ctx, server, mode, variant})
}

func (p *faultyReplica) Propose(ctx gorums.ServerContext, msg *brbv1.PhaseMessage) {
	ctx.Release()
	if p.mode != "wrong-echo" || msg.GetId() == nil || ctx.SenderID() != msg.GetId().GetOrigin() {
		return
	}
	value := append([]byte(nil), msg.GetValue()...)
	if len(value) == 0 {
		value = []byte{1}
	} else {
		value[0] ^= 1
	}
	// The fault intentionally sends a value different from the proposal.
	_ = brbv1.Echo(p.server.PeerConfig().Context(p.ctx), brbv1.PhaseMessage_builder{
		Id: msg.GetId(), Value: value, Variant: p.variant,
	}.Build()).Send()
}

func (*faultyReplica) Echo(gorums.ServerContext, *brbv1.PhaseMessage)  {}
func (*faultyReplica) Ready(gorums.ServerContext, *brbv1.PhaseMessage) {}
func (*faultyReplica) QEcho(gorums.ServerContext, *brbv1.PhaseMessage) {}

func equivocate(ctx context.Context, server *gorums.Server, variant brbv1.Variant) error {
	id := brbv1.BroadcastID_builder{Origin: server.NodeID(), Sequence: 1}.Build()
	// Alternate proposals across recipients, then supply one ECHO for A.
	for i, peer := range server.PeerConfig() {
		value := []byte("A")
		if i%2 != 0 {
			value = []byte("B")
		}
		msg := brbv1.PhaseMessage_builder{Id: id, Value: value, Variant: variant}.Build()
		if err := brbv1.Propose(gorums.Config{peer}.Context(ctx), msg).Send(); err != nil {
			return fmt.Errorf("faulty proposal: %w", err)
		}
	}
	return brbv1.Echo(server.PeerConfig().Context(ctx), brbv1.PhaseMessage_builder{
		Id: id, Value: []byte("A"), Variant: variant,
	}.Build()).Send()
}

func runFault(ctx context.Context, node *benchmarkNode, opts Options) error {
	if opts.fault == "equivocate" {
		if err := equivocate(ctx, node.server, opts.variant()); err != nil {
			return err
		}
	}
	if err := waitGrace(ctx, opts.Duration); err != nil {
		return err
	}
	if _, err := finishPeers(ctx, node.server, opts.selfID, node.control, node.done, completionTimeout(len(opts.Remotes))); err != nil {
		return err
	}
	// Faulty replicas produce no performance report. Only correct origins are
	// measured, and their report labels identify the selected fault scenario.
	return waitGrace(ctx, time.Second)
}
