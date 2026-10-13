package bench

import (
	"context"
	"fmt"
	"time"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
)

// Faults keep their own identity. These are controlled faulty behaviors, not
// protection against a process that forges another replica's sender field.
type faultyReplica struct {
	ctx    context.Context
	server *gorums.Server
	mode   string
}

func registerFault(ctx context.Context, server *gorums.Server, mode string) {
	brbv1.RegisterBrachaServer(server, &faultyReplica{ctx, server, mode})
}

func (p *faultyReplica) Propose(ctx gorums.ServerContext, msg *brbv1.PhaseMessage) {
	ctx.Release()
	if p.mode != "wrong-echo" || msg.GetId() == nil || msg.GetSender() != msg.GetId().GetOrigin() {
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
		Id: msg.GetId(), Sender: p.server.NodeID(), Value: value,
	}.Build()).Send()
}

func (*faultyReplica) Echo(gorums.ServerContext, *brbv1.PhaseMessage)  {}
func (*faultyReplica) Ready(gorums.ServerContext, *brbv1.PhaseMessage) {}

func equivocate(ctx context.Context, server *gorums.Server) error {
	id := brbv1.BroadcastID_builder{Origin: server.NodeID(), Sequence: 1}.Build()
	// Alternate proposals across recipients, then supply one ECHO for A.
	for i, peer := range server.PeerConfig() {
		value := []byte("A")
		if i%2 != 0 {
			value = []byte("B")
		}
		msg := brbv1.PhaseMessage_builder{Id: id, Sender: server.NodeID(), Value: value}.Build()
		if err := brbv1.Propose(gorums.Config{peer}.Context(ctx), msg).Send(); err != nil {
			return fmt.Errorf("faulty proposal: %w", err)
		}
	}
	return brbv1.Echo(server.PeerConfig().Context(ctx), brbv1.PhaseMessage_builder{
		Id: id, Sender: server.NodeID(), Value: []byte("A"),
	}.Build()).Send()
}

func runFault(ctx context.Context, node *benchmarkNode, opts Options) error {
	if opts.fault == "equivocate" {
		if err := equivocate(ctx, node.server); err != nil {
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
