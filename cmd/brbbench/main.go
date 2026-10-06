package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/relab/gorums"
	"github.com/relab/gorums/benchkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const benchmarkName = "BrachaLocalDelivery"

type options struct {
	*benchkit.StandardFlags
	f      int
	selfID uint32
}

func main() {
	opts, err := parseOptions(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err == nil {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		err = run(ctx, opts)
	}
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func parseOptions(args []string, output io.Writer) (options, error) {
	fs := flag.NewFlagSet("brbbench", flag.ContinueOnError)
	fs.SetOutput(output)
	opts := options{StandardFlags: benchkit.RegisterFlags(fs)}
	fs.IntVar(&opts.f, "f", -1, "required fault bound (n must be greater than 3*f)")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() != 0 {
		return opts, errors.New("brbbench: unexpected positional arguments")
	}
	if err := opts.Options().Validate(); err != nil {
		return opts, err
	}
	if opts.Payload > 1<<20 || opts.Output == "" || !opts.Benchmarks.MatchString(benchmarkName) {
		return opts, errors.New("brbbench: requires -output, a selector matching BrachaLocalDelivery, and payload <= 1 MiB")
	}
	// Do not silently accept features this first benchmark does not implement.
	if opts.StreamMode != "dual" || opts.RateStep != 0 || opts.RateStepMax != 0 ||
		opts.CallTimeout != 0 || opts.FaultKillAfter != 0 || opts.CPUProfile != "" || opts.MemProfile != "" || opts.Trace != "" {
		return opts, errors.New("brbbench: dedup, rate ramps, call timeouts, fault injection, and profiling are not supported")
	}
	self, err := endpoint(opts.Self)
	if err != nil {
		return opts, err
	}
	opts.Self = self
	seen := make(map[string]bool)
	for i, peer := range opts.Remotes {
		peer, err = endpoint(peer)
		if err != nil {
			return opts, err
		}
		if seen[peer] {
			return opts, fmt.Errorf("brbbench: duplicate peer %s", peer)
		}
		seen[peer] = true
		opts.Remotes[i] = peer
		if peer == self {
			opts.selfID = uint32(i + 1)
		}
	}
	if opts.selfID == 0 {
		return opts, errors.New("brbbench: -self must occur in -remotes, in the same order on every node")
	}
	if _, err := brb.NewMachine(len(opts.Remotes), opts.f); err != nil {
		return opts, err
	}
	return opts, nil
}

// Sweep supplies numeric peer addresses. Requiring them avoids host-specific
// DNS results changing the membership or binding only to loopback on a cluster.
func endpoint(value string) (string, error) {
	addr, err := netip.ParseAddrPort(value)
	if err != nil || addr.Port() == 0 || addr.Addr().IsUnspecified() {
		return "", fmt.Errorf("brbbench: %q must be a non-wildcard IP:port with a positive port", value)
	}
	return addr.String(), nil
}

func run(parent context.Context, opts options) error {
	// Bound startup, measurement, and peer completion, including stalled sends.
	ctx, cancel := context.WithTimeout(parent, opts.Duration+45*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", opts.Self)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := gorums.NewServer(gorums.WithPeers(opts.selfID, gorums.WithNodeList(opts.Remotes),
		gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true))),
	))
	defer func() { cancel(); server.Stop() }()
	control := benchkit.NewControl()
	done := control.ArmDone(len(opts.Remotes))
	benchkit.RegisterControlServer(server, control)
	var mu sync.Mutex
	waiters := make(map[uint64]chan time.Time)
	node, err := brb.NewGorumsNode(ctx, server, opts.f, func(d brb.Delivery) {
		if uint32(d.ID.Origin) != opts.selfID {
			return
		}
		deliveredAt := time.Now()
		mu.Lock()
		if ch := waiters[d.ID.Sequence]; ch != nil {
			delete(waiters, d.ID.Sequence)
			ch <- deliveredAt
		}
		mu.Unlock()
	})
	if err != nil {
		return err
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	readyCtx, readyCancel := context.WithTimeout(ctx, 30*time.Second)
	err = server.WaitForPeers(readyCtx, func(c gorums.Config) bool { return len(c) == len(opts.Remotes) })
	readyCancel()
	if err != nil {
		return fmt.Errorf("brbbench: wait for peers: %w", err)
	}
	var sequence atomic.Uint64
	value := bytes.Repeat([]byte{'x'}, opts.Payload)
	settings := opts.Options()
	settings.NumNodes, settings.Remote = len(opts.Remotes), true
	results, err := benchkit.Run(opts.Benchmarks, settings, []benchkit.Bench{{
		Name: benchmarkName,
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
					mu.Lock()
					waiters[id] = ch
					mu.Unlock()
					defer func() { mu.Lock(); delete(waiters, id); mu.Unlock() }()
					startedAt := time.Now()
					if err := node.Broadcast(id, value); err != nil {
						return err
					}
					select {
					case deliveredAt := <-ch:
						measurement.Stats.AddLatency(deliveredAt.Sub(startedAt))
						return nil
					case <-ctx.Done():
						return ctx.Err()
					case err := <-serveErr:
						cancel()
						return fmt.Errorf("brbbench: server stopped: %v", err)
					}
				})
			if err != nil {
				return nil, err
			}
			return measurement.Finish(), nil
		},
	}})
	if err != nil {
		return err
	}
	if ctx.Err() != nil || results[0].GetFailedOps() != 0 || results[0].GetTotalOps() == 0 {
		return fmt.Errorf("brbbench: incomplete run: context=%v, failed=%d, completed=%d",
			ctx.Err(), results[0].GetFailedOps(), results[0].GetTotalOps())
	}
	// Keep serving until every origin has finished its own workload. This is
	// benchmark coordination, not a BRB phase or proof of delivery everywhere.
	if err := benchkit.Done(server.PeerConfig().Context(ctx), benchkit.DoneRequest_builder{SenderId: opts.selfID}.Build()).Send(); err != nil {
		return err
	}
	finishCtx, finishCancel := context.WithTimeout(ctx, 10*time.Second)
	defer finishCancel()
	select {
	case <-done:
	case <-finishCtx.Done():
		return fmt.Errorf("brbbench: peers did not finish: %w", finishCtx.Err())
	}
	// Allow trailing protocol messages to drain before peers close connections.
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	label := fmt.Sprintf("node %d f=%d", opts.selfID, opts.f)
	return benchkit.WriteLabeledReport(results, label, opts.Output)
}
