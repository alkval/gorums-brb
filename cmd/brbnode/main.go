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
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

type options struct {
	id          uint
	f           int
	peers       []string // list position determines the process ID, starting at 1
	broadcast   string   // optional value, repeated with distinct sequence numbers
	count       int
	interval    time.Duration
	payloadSize int
}

func main() {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	opts, err := parseOptions(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		logger.Print(err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = run(ctx, opts, logger)
	stop()
	if err != nil {
		logger.Print(err)
		os.Exit(1)
	}
}

func parseOptions(args []string, output io.Writer) (options, error) {
	var opts options
	var peers string
	flags := flag.NewFlagSet("brbnode", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.UintVar(&opts.id, "id", 0, "this node's ID (1 through the number of peers)")
	flags.IntVar(&opts.f, "f", -1, "fault bound, required (n must be greater than 3*f)")
	flags.StringVar(&peers, "peers", "", "comma-separated host:port addresses, ordered by node ID")
	flags.StringVar(&opts.broadcast, "broadcast", "", "nonempty value to broadcast after all peers connect")
	flags.IntVar(&opts.count, "count", 1, "number of broadcasts, using sequences 1 through count")
	flags.DurationVar(&opts.interval, "interval", 0, "delay after each submission before the next (e.g. 100ms)")
	flags.IntVar(&opts.payloadSize, "payload-size", 0, "generated payload size in bytes (1 through 1048576), instead of -broadcast")
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	if flags.NArg() != 0 {
		return opts, errors.New("brbnode: unexpected positional arguments")
	}
	if opts.count < 1 {
		return opts, errors.New("brbnode: -count must be positive")
	}
	if opts.interval < 0 {
		return opts, errors.New("brbnode: -interval must not be negative")
	}
	if opts.payloadSize < 0 || opts.payloadSize > 1<<20 {
		return opts, errors.New("brbnode: -payload-size must be from 0 to 1048576")
	}
	if opts.payloadSize > 0 && opts.broadcast != "" {
		return opts, errors.New("brbnode: use either -broadcast or -payload-size, not both")
	}
	if (opts.count > 1 || opts.interval > 0) && opts.broadcast == "" && opts.payloadSize == 0 {
		return opts, errors.New("brbnode: -count greater than 1 or -interval requires -broadcast or -payload-size")
	}
	if strings.TrimSpace(peers) == "" {
		return opts, errors.New("brbnode: -peers is required")
	}
	opts.peers = strings.Split(peers, ",")
	seen := make(map[string]bool)
	for i, peer := range opts.peers {
		peer = strings.TrimSpace(peer)
		host, port, err := net.SplitHostPort(peer)
		if err != nil || host == "" {
			return opts, fmt.Errorf("brbnode: peer %q must be host:port", peer)
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return opts, fmt.Errorf("brbnode: peer %q needs a port from 1 to 65535", peer)
		}
		// Normalize before passing addresses to Gorums, which rejects duplicate
		// endpoints even when they are written differently.
		addr, err := net.ResolveTCPAddr("tcp", peer)
		if err != nil {
			return opts, fmt.Errorf("brbnode: resolve peer %q: %w", peer, err)
		}
		if addr.IP.IsUnspecified() {
			return opts, fmt.Errorf("brbnode: peer %q needs a reachable host, not a wildcard", peer)
		}
		peer = addr.String()
		if seen[peer] {
			return opts, fmt.Errorf("brbnode: duplicate peer %q", peer)
		}
		seen[peer] = true
		opts.peers[i] = peer
	}
	if opts.id == 0 || opts.id > uint(len(opts.peers)) {
		return opts, fmt.Errorf("brbnode: -id must be from 1 to %d", len(opts.peers))
	}
	if _, err := brb.NewStateMachine(len(opts.peers), opts.f); err != nil {
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
		return fmt.Errorf("brbnode: listen: %w", err)
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
	if opts.broadcast != "" || opts.payloadSize > 0 {
		// This demo assumes established connections. Wait for the full
		// membership before asking the adapter to start the broadcast.
		logger.Printf("node %d waiting for all %d peers before broadcasting", opts.id, len(opts.peers))
		readyCtx, readyCancel := context.WithTimeout(runCtx, 30*time.Second)
		err = server.WaitForPeers(readyCtx, func(config gorums.Config) bool {
			return len(config) == len(opts.peers)
		})
		readyCancel()
		if err != nil {
			return fmt.Errorf("brbnode: wait for peers: %w", err)
		}
		value := []byte(opts.broadcast)
		if opts.payloadSize > 0 {
			value = bytes.Repeat([]byte{'x'}, opts.payloadSize)
		}
		for i := range opts.count {
			if i > 0 && opts.interval > 0 {
				timer := time.NewTimer(opts.interval)
				select {
				case <-runCtx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
				}
			}
			sequence := uint64(i) + 1
			if err := node.Broadcast(sequence, value); err != nil {
				return fmt.Errorf("brbnode: broadcast sequence %d: %w", sequence, err)
			}
			logBroadcast(logger, opts.id, "submitted", brb.BroadcastID{Origin: brb.ProcessID(opts.id), Sequence: sequence}, value)
		}
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-serveErr:
		return fmt.Errorf("brbnode: server stopped unexpectedly: %v", err)
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
