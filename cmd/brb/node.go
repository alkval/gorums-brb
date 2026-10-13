package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/alkval/gorums-brb/internal/cluster"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

type options struct {
	id        uint
	f         int
	peers     []string // list position determines the process ID, starting at 1
	broadcast string   // optional value, repeated with distinct sequence numbers
	count     int
}

func parseOptions(args []string, output io.Writer) (options, error) {
	var opts options
	var peers string
	flags := flag.NewFlagSet("brb", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.UintVar(&opts.id, "id", 0, "this node's ID (1 through the number of peers)")
	flags.IntVar(&opts.f, "f", -1, "fault bound (default: (n-1)/3)")
	flags.StringVar(&peers, "peers", "", "comma-separated host:port addresses, ordered by node ID")
	flags.StringVar(&opts.broadcast, "broadcast", "", "nonempty value to broadcast after all peers connect")
	flags.IntVar(&opts.count, "count", 1, "number of broadcasts, using sequences 1 through count")
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	if flags.NArg() != 0 {
		return opts, errors.New("brb: unexpected positional arguments")
	}
	if opts.count < 1 {
		return opts, errors.New("brb: -count must be positive")
	}
	if opts.count > 1 && opts.broadcast == "" {
		return opts, errors.New("brb: -count greater than 1 requires -broadcast")
	}
	if strings.TrimSpace(peers) == "" {
		return opts, errors.New("brb: -peers is required")
	}
	var err error
	opts.peers, err = cluster.Peers(strings.Split(peers, ","), true)
	if err != nil {
		return opts, err
	}
	if opts.id == 0 || opts.id > uint(len(opts.peers)) {
		return opts, fmt.Errorf("brb: -id must be from 1 to %d", len(opts.peers))
	}
	if opts.f, err = cluster.FaultBound(len(opts.peers), opts.f); err != nil {
		return opts, err
	}
	return opts, nil
}

func run(ctx context.Context, opts options, logger *log.Logger) (err error) {
	peers := gorums.WithNodeList(opts.peers)
	// Keep connection retries short while peers are started in separate terminals.
	connectionBackoff := backoff.DefaultConfig
	connectionBackoff.BaseDelay = 100 * time.Millisecond
	connectionBackoff.MaxDelay = time.Second
	listener, err := net.Listen("tcp", opts.peers[opts.id-1])
	if err != nil {
		return fmt.Errorf("brb: listen: %w", err)
	}
	defer listener.Close()
	server := gorums.NewServer(gorums.WithPeers(uint32(opts.id), peers,
		gorums.WithBackoff(connectionBackoff),
		gorums.WithGRPCDialOptions(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// Peers start separately. Let initial streams wait for their servers
			// rather than fail before the other terminals have started.
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		),
	))
	runCtx, cancel := context.WithCancel(ctx)
	defer func() {
		// Cancel protocol work before closing the server and its connections.
		cancel()
		// A requested shutdown is not a failed broadcast or startup.
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			err = nil
		}
		server.Stop()
		if ctx.Err() != nil {
			logger.Printf("node %d stopping", opts.id)
		}
	}()
	node, err := brb.NewReplica(runCtx, server, opts.f, func(d brb.Delivery) {
		logBroadcast(logger, opts.id, "delivered", d.ID, d.Value)
	})
	if err != nil {
		return err
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	logger.Printf("node %d listening on %s (n=%d f=%d)", opts.id, listener.Addr(), len(opts.peers), opts.f)
	if opts.broadcast != "" {
		// This demo assumes established connections. Wait for the full
		// membership before asking the adapter to start the broadcast.
		logger.Printf("node %d waiting for all %d peers before broadcasting", opts.id, len(opts.peers))
		readyCtx, readyCancel := context.WithTimeout(runCtx, 30*time.Second)
		err = server.WaitForPeers(readyCtx, func(config gorums.Config) bool {
			return len(config) == len(opts.peers)
		})
		readyCancel()
		if err != nil {
			return fmt.Errorf("brb: wait for peers: %w", err)
		}
		value := []byte(opts.broadcast)
		for i := range opts.count {
			sequence := uint64(i) + 1
			if err := node.Broadcast(sequence, value); err != nil {
				return fmt.Errorf("brb: broadcast sequence %d: %w", sequence, err)
			}
			logBroadcast(logger, opts.id, "submitted", brb.BroadcastID{Origin: brb.ProcessID(opts.id), Sequence: sequence}, value)
		}
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-serveErr:
		return fmt.Errorf("brb: server stopped unexpectedly: %w", err)
	}
}

// Keep larger workloads from printing their entire payload on every delivery.
func logBroadcast(logger *log.Logger, nodeID uint, event string, id brb.BroadcastID, value []byte) {
	if len(value) > 64 {
		logger.Printf("node %d %s origin=%d sequence=%d bytes=%d", nodeID, event, id.Origin, id.Sequence, len(value))
		return
	}
	logger.Printf("node %d %s origin=%d sequence=%d value=%q", nodeID, event, id.Origin, id.Sequence, value)
}
