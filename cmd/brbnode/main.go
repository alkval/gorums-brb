package main

import (
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
	id        uint
	f         int
	peers     []string // list position determines the process ID, starting at 1
	broadcast string   // optional one-shot value, using sequence 1
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
	flags.StringVar(&opts.broadcast, "broadcast", "", "broadcast one nonempty value after all peers connect")
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	if flags.NArg() != 0 {
		return opts, errors.New("brbnode: unexpected positional arguments")
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
	if _, err := brb.NewMachine(len(opts.peers), opts.f); err != nil {
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
	system, err := gorums.NewSystem(opts.peers[opts.id-1],
		gorums.WithServerOptions(gorums.WithConfig(uint32(opts.id), peers)),
		gorums.WithOutboundNodes(peers),
		gorums.WithBackoff(connectionBackoff),
		gorums.WithDialOptions(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// Peers start separately. Let initial streams wait for their servers
			// rather than fail before the other terminals have started.
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		),
	)
	if err != nil {
		return fmt.Errorf("brbnode: create system: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer func() {
		// Cancel protocol work before closing the server and its connections.
		cancel()
		// A requested shutdown is not a failed broadcast or startup.
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			err = nil
		}
		err = errors.Join(err, system.Stop())
		if ctx.Err() != nil {
			logger.Printf("node %d stopping", opts.id)
		}
	}()
	node, err := brb.NewGorumsNode(runCtx, system, opts.f, func(d brb.Delivery) {
		logger.Printf("node %d delivered origin=%d sequence=%d value=%q",
			opts.id, d.ID.Origin, d.ID.Sequence, d.Value)
	})
	if err != nil {
		return err
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- system.Serve() }()
	logger.Printf("node %d listening on %s (n=%d f=%d)", opts.id, system.Addr(), len(opts.peers), opts.f)
	if opts.broadcast != "" {
		// This demo assumes established connections. Wait for the full
		// membership before asking the adapter to start the broadcast.
		logger.Printf("node %d waiting for all %d peers before broadcasting", opts.id, len(opts.peers))
		readyCtx, readyCancel := context.WithTimeout(runCtx, 30*time.Second)
		err = system.WaitForConfig(readyCtx, func(config gorums.Configuration) bool {
			return len(config) == len(opts.peers)
		})
		readyCancel()
		if err != nil {
			return fmt.Errorf("brbnode: wait for peers: %w", err)
		}
		if err := node.Broadcast(1, []byte(opts.broadcast)); err != nil {
			return fmt.Errorf("brbnode: broadcast: %w", err)
		}
		logger.Printf("node %d submitted origin=%d sequence=1 value=%q", opts.id, opts.id, opts.broadcast)
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-serveErr:
		return fmt.Errorf("brbnode: server stopped unexpectedly: %v", err)
	}
}
