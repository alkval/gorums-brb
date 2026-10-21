// Package bench runs origin-local delivery measurements through Gorums benchkit.
package bench

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/alkval/gorums-brb/internal/cluster"
	"github.com/relab/gorums"
	"github.com/relab/gorums/benchkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const benchmarkName = "BrachaLocalDelivery"

type Options struct {
	*benchkit.StandardFlags
	f          int
	selfID     uint32
	failFast   bool
	fault      string
	faultNode  uint
	optimistic bool
}

func (o Options) benchmarkName() string {
	if o.optimistic {
		return "OptimisticLocalDelivery"
	}
	return benchmarkName
}

func ParseOptions(args []string, output io.Writer) (Options, error) {
	fs := flag.NewFlagSet("brb -mode=bench", flag.ContinueOnError)
	fs.SetOutput(output)
	opts := Options{StandardFlags: benchkit.RegisterFlags(fs)}
	fs.IntVar(&opts.f, "f", -1, "fault bound (default: (n-1)/3)")
	fs.BoolVar(&opts.optimistic, "optimistic", false, "use experimental rotating committees without recovery (all nodes)")
	fs.BoolVar(&opts.failFast, "fail-fast", false, "report unavailable-peer sends promptly for fault diagnostics; correct peers must already be connected")
	fs.StringVar(&opts.fault, "fault", "none", "faulty replica behavior: none, silent, equivocate, wrong-echo")
	fs.UintVar(&opts.faultNode, "fault-node", 0, "faulty replica ID (default: last replica)")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() != 0 {
		return opts, errors.New("brb bench: unexpected positional arguments")
	}
	if err := opts.Options().Validate(); err != nil {
		return opts, err
	}
	if opts.Payload > 1<<20 || opts.Output == "" || !opts.Benchmarks.MatchString(opts.benchmarkName()) {
		return opts, fmt.Errorf("brb bench: requires -output, a selector matching %s, and payload <= 1 MiB", opts.benchmarkName())
	}
	// Do not silently accept features this first benchmark does not implement.
	if opts.StreamMode != "dual" || opts.RateStep != 0 || opts.RateStepMax != 0 ||
		opts.CallTimeout != 0 || opts.CPUProfile != "" || opts.MemProfile != "" || opts.Trace != "" {
		return opts, errors.New("brb bench: dedup, rate ramps, call timeouts, and profiling are not supported")
	}
	self, err := cluster.Peers([]string{opts.Self}, false)
	if err != nil {
		return opts, err
	}
	opts.Self = self[0]
	opts.Remotes, err = cluster.Peers(opts.Remotes, false)
	if err != nil {
		return opts, err
	}
	for i, peer := range opts.Remotes {
		if peer == opts.Self {
			opts.selfID = uint32(i + 1)
		}
	}
	if opts.selfID == 0 {
		return opts, errors.New("brb: -self must occur in -remotes, in the same order on every node")
	}
	if opts.f, err = cluster.FaultBound(len(opts.Remotes), opts.f); err != nil {
		return opts, err
	}
	if opts.faultNode == 0 {
		opts.faultNode = uint(len(opts.Remotes))
	}
	if opts.faultNode > uint(len(opts.Remotes)) {
		return opts, errors.New("brb: fault node is outside membership")
	}
	switch opts.fault {
	case "none", "silent", "equivocate", "wrong-echo":
	default:
		return opts, errors.New("brb: unknown fault behavior")
	}
	if opts.FaultKillAfter < 0 || (opts.FaultKillAfter > 0 && opts.FaultKillAfter >= opts.Duration) {
		return opts, errors.New("brb: fault-kill-after must be positive and shorter than the workload")
	}
	if opts.FaultKillAfter > 0 || opts.fault != "none" {
		if opts.optimistic {
			return opts, errors.New("brb: optimistic benchmark supports fault-free comparisons only; recovery is not implemented")
		}
		if opts.f == 0 {
			return opts, errors.New("brb: injected faults require f >= 1")
		}
		opts.failFast = true
	}
	return opts, nil
}

// Run starts a fixed group member and records completed origin broadcasts.
func Run(parent context.Context, opts Options) error {
	ctx, cancel := context.WithTimeout(parent, opts.Duration+startupTimeout(len(opts.Remotes))+completionTimeout(len(opts.Remotes))+5*time.Second)
	defer cancel()
	tracker := &deliveryTracker{self: opts.selfID, waiters: make(map[uint64]chan time.Time)}
	node, err := startNode(ctx, opts, tracker.delivered)
	if err != nil {
		return err
	}
	defer func() {
		cancel()
		node.server.Stop()
		node.replica.LogMulticastFailures()
	}()
	if err := awaitPeers(ctx, node, opts); err != nil {
		return err
	}
	if uint(opts.selfID) == opts.faultNode && opts.FaultKillAfter > 0 {
		timer := benchkit.ArmFaultInjection(opts.FaultKillAfter)
		defer timer.Stop()
	}
	if uint(opts.selfID) == opts.faultNode && opts.fault != "none" {
		return runFault(ctx, node, opts)
	}
	results, failures, err := measure(ctx, cancel, node, opts, tracker)
	if err != nil {
		return err
	}
	missing, err := finishPeers(ctx, node.server, opts.selfID, node.control, node.done, completionTimeout(len(opts.Remotes)))
	if err != nil {
		return err
	}
	if err := waitGrace(ctx, time.Second); err != nil {
		return err
	}
	// Stop accepting traffic before checking the run's window status.
	cancel()
	node.server.Stop()
	if err := node.replica.WindowError(); err != nil {
		return fmt.Errorf("brb bench: invalid run: %w", err)
	}
	logTraffic(opts.selfID, node.replica)
	label := fmt.Sprintf("node %d f=%d", opts.selfID, opts.f)
	if opts.optimistic {
		label += " optimistic=true"
	}
	if opts.failFast {
		label += " fail-fast=true"
	}
	if opts.fault != "none" || opts.FaultKillAfter > 0 {
		label += fmt.Sprintf(" fault=%s fault-node=%d kill-after=%s", opts.fault, opts.faultNode, opts.FaultKillAfter)
	}
	if failures != 0 || len(missing) != 0 {
		label += fmt.Sprintf(" initial-send-failures=%d missing-done=%v", failures, missing)
		benchkit.Printf("brb bench: %s\n", label)
	}
	return benchkit.WriteLabeledReport(results, label, opts.Output)
}

type benchmarkNode struct {
	server        *gorums.Server
	replica       *brb.Replica
	control       *benchkit.Control
	done          <-chan struct{}
	ready, probed chan struct{}
	serveErr      <-chan error
}

type deliveryTracker struct {
	self    uint32
	mu      sync.Mutex
	waiters map[uint64]chan time.Time
}

func (t *deliveryTracker) delivered(d brb.Delivery) {
	if uint32(d.ID.Origin) != t.self {
		return
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if ch := t.waiters[d.ID.Sequence]; ch != nil {
		delete(t.waiters, d.ID.Sequence)
		ch <- now
	}
}

func startNode(ctx context.Context, opts Options, deliver func(brb.Delivery)) (*benchmarkNode, error) {
	listener, err := net.Listen("tcp", opts.Self)
	if err != nil {
		return nil, err
	}
	ready := make(chan struct{})
	server := gorums.NewServer(gorums.WithPeers(opts.selfID, gorums.WithNodeList(opts.Remotes),
		gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(!opts.failFast))),
	), gorums.WithServerInterceptors(func(sc gorums.ServerContext, msg *gorums.Message, next gorums.Handler) (*gorums.Message, error) {
		if strings.HasPrefix(msg.GetMethod(), "brb.v1.Bracha.") {
			// Startup probes must still run while early BRB messages wait.
			sc.Release()
			select {
			case <-ready:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return next(sc, msg)
	}))
	control := benchkit.NewControl()
	done := control.ArmDone(len(opts.Remotes))
	probed := make(chan struct{})
	benchkit.RegisterControlServer(server, readyControl{Control: control, probed: probed})

	newReplica := brb.NewReplica
	if opts.optimistic {
		newReplica = brb.NewOptimisticReplica
	}
	replica, err := newReplica(ctx, server, opts.f, deliver)
	if err != nil {
		_ = listener.Close()
		server.Stop()
		return nil, err
	}
	if uint(opts.selfID) == opts.faultNode && opts.fault != "none" {
		registerFault(ctx, server, opts.fault)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	benchkit.Logf("brb bench: node %d listening on %s\n", opts.selfID, listener.Addr())
	return &benchmarkNode{server, replica, control, done, ready, probed, serveErr}, nil
}

func startupTimeout(n int) time.Duration { return 30*time.Second + time.Duration(n)*time.Second }
func completionTimeout(n int) time.Duration {
	return 10*time.Second + time.Duration(n)*100*time.Millisecond
}

func awaitPeers(ctx context.Context, node *benchmarkNode, opts Options) error {
	readyCtx, readyCancel := context.WithTimeout(ctx, startupTimeout(len(opts.Remotes)))
	err := node.server.WaitForPeers(readyCtx, func(c gorums.Config) bool { return len(c) == len(opts.Remotes) })
	if err == nil {
		err = probePeers(readyCtx, node.server.PeerConfig())
	}
	close(node.probed)
	if err == nil {
		// Each peer's Start waits for its own probes. All replies therefore
		// establish that the entire group has finished its startup checks,
		// so a connected but unready replica cannot accumulate a workload.
		_, err = benchkit.Start(node.server.PeerConfig().Context(readyCtx), benchkit.StartRequest_builder{}.Build()).All()
	}
	readyCancel()
	if err != nil {
		return fmt.Errorf("brb bench: wait for peers: %w", err)
	}
	close(node.ready)
	benchkit.Logf("brb bench: node %d connected to all peers, starting measurement\n", opts.selfID)
	return nil
}

func measure(ctx context.Context, cancel context.CancelFunc, node *benchmarkNode, opts Options, tracker *deliveryTracker) ([]*benchkit.Result, uint64, error) {
	var sequence atomic.Uint64
	var sendFailures atomic.Uint64
	value := bytes.Repeat([]byte{'x'}, opts.Payload)
	settings := opts.Options()
	settings.NumNodes, settings.Remote = len(opts.Remotes), true
	results, err := benchkit.Run(opts.Benchmarks, settings, []benchkit.Bench{{
		Name: opts.benchmarkName(),
		Run: func(o benchkit.Options) (*benchkit.Result, error) {
			measurement := benchkit.StartMeasurement(o)
			defer measurement.Abandon()
			start := time.Now()
			end := start.Add(o.Duration)
			// Stop pacing at the deadline without cancelling broadcasts already
			// started. Those still use the enclosing run context to finish.
			phaseCtx, phaseCancel := context.WithDeadline(ctx, end)
			defer phaseCancel()
			err := benchkit.RunPhase(phaseCtx, o.Workers, o.Rate, start, end,
				func() error {
					if phaseCtx.Err() != nil || !time.Now().Before(end) {
						return benchkit.ErrRunOver
					}
					id := sequence.Add(1)
					ch := make(chan time.Time, 1)
					tracker.mu.Lock()
					tracker.waiters[id] = ch
					tracker.mu.Unlock()
					defer func() { tracker.mu.Lock(); delete(tracker.waiters, id); tracker.mu.Unlock() }()
					startedAt := time.Now()
					if err := node.replica.Broadcast(id, value); err != nil {
						if !errors.Is(err, gorums.ErrSendFailure) {
							return err
						}
						sendFailures.Add(1)
						// A failed send to one peer does not establish whether the
						// origin can deliver. Wait for the actual delivery below.
					}
					select {
					case deliveredAt := <-ch:
						measurement.Stats.AddLatency(deliveredAt.Sub(startedAt))
						return nil
					case <-ctx.Done():
						return ctx.Err()
					case err := <-node.serveErr:
						cancel()
						return fmt.Errorf("brb bench: server stopped: %w", err)
					}
				})
			if err != nil {
				return nil, err
			}
			return measurement.Finish(), nil
		},
	}})
	if err != nil {
		return nil, 0, err
	}
	if ctx.Err() != nil || results[0].GetFailedOps() != 0 || results[0].GetTotalOps() == 0 {
		if err := ctx.Err(); err != nil {
			return nil, 0, fmt.Errorf("brb bench: incomplete run: %w", err)
		}
		return nil, 0, fmt.Errorf("brb bench: incomplete run: failed=%d, completed=%d", results[0].GetFailedOps(), results[0].GetTotalOps())
	}
	return results, sendFailures.Load(), nil
}

// logTraffic records attempted protocol sends once per phase. Sweep keeps each
// node's stderr in the per-run log, so byte accounting needs no extra result
// file. The totals cover the whole run, including the final drain.
func logTraffic(selfID uint32, replica *brb.Replica) {
	for _, p := range []struct {
		name  string
		phase brb.Phase
	}{{"propose", brb.PhasePropose}, {"echo", brb.PhaseEcho}, {"ready", brb.PhaseReady}, {"qecho", brb.PhaseQEcho}} {
		t := replica.Traffic(p.phase)
		benchkit.Printf("brb bench: node %d traffic phase=%s calls=%d remote_msgs=%d remote_bytes=%d local_msgs=%d local_bytes=%d\n",
			selfID, p.name, t.Calls, t.RemoteMessages, t.RemoteBytes, t.LocalMessages, t.LocalBytes)
	}
}

func waitGrace(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start is used as a startup barrier only. The measurement has its own Stats.
type readyControl struct {
	*benchkit.Control
	probed <-chan struct{}
}

func (c readyControl) Start(ctx gorums.ServerContext, _ *benchkit.StartRequest) (*benchkit.StartResponse, error) {
	ctx.Release()
	select {
	case <-c.probed:
		return benchkit.StartResponse_builder{}.Build(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A stream can appear connected before its first successful round trip. Verify
// each outbound path before beginning the workload. ClockSync is used only
// as a harmless probe; its timestamp does not enter the latency measurements.
func probePeers(ctx context.Context, config gorums.Config) error {
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, time.Second)
		_, err := benchkit.ClockSync(config.Context(attemptCtx), benchkit.ClockSyncRequest_builder{}.Build()).All()
		cancel()
		if err == nil {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Done is advisory: a crashed peer cannot announce completion. Keep serving
// until every peer signals or the grace expires, and record missing signals.
func finishPeers(ctx context.Context, server *gorums.Server, selfID uint32, control *benchkit.Control, done <-chan struct{}, grace time.Duration) ([]uint32, error) {
	finishCtx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()
	if err := benchkit.Done(server.PeerConfig().Context(finishCtx), benchkit.DoneRequest_builder{SenderId: selfID}.Build()).Send(); err != nil {
		benchkit.Logf("brb bench: node %d completion send: %v\n", selfID, err)
	}
	select {
	case <-done:
	case <-finishCtx.Done():
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return control.MissingDone(), nil
}
