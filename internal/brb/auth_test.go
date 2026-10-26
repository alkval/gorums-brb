package brb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alkval/gorums-brb/internal/peertls"
	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// generateCreds creates a project CA and per-replica credentials for n local
// replicas. Local listeners use 127.0.0.1, so the port does not matter.
func generateCreds(t *testing.T, n int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "creds")
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = "127.0.0.1:0"
	}
	if err := peertls.Generate(dir, addrs, time.Hour); err != nil {
		t.Fatal(err)
	}
	return dir
}

func credDir(creds string, id uint32) string {
	return filepath.Join(creds, "node-"+strconv.FormatUint(uint64(id), 10))
}

// tlsNodes starts n replicas whose peers carry TLS identities, so Gorums
// verifies every peer stream. Server id presents the certificate of certOf(id),
// normally its own. The servers are not started.
func tlsNodes(t *testing.T, creds string, n, f int, certOf func(uint32) uint32, extra ...gorums.LocalServerOption) ([]*Replica, []*gorums.Server, []chan Delivery) {
	t.Helper()
	server := make(map[uint32]credentials.TransportCredentials)
	client := make(map[uint32]credentials.TransportCredentials)
	for id := uint32(1); id <= uint32(n); id++ {
		var err error
		if server[id], client[id], err = peertls.Load(credDir(creds, certOf(id)), certOf(id)); err != nil {
			t.Fatal(err)
		}
	}
	opts := append([]gorums.LocalServerOption{
		gorums.WithLocalNodes(func(id gorums.ID, addr string) gorums.NodeAddress {
			return peertls.NodeFor(id, addr)
		}),
		gorums.WithLocalServerOptionsFunc(func(id gorums.ID) []gorums.ServerOption {
			return []gorums.ServerOption{gorums.WithGRPCServerOptions(grpc.Creds(server[id]))}
		}),
		gorums.WithLocalDialOptionsFunc(func(id gorums.ID) []gorums.DialOption {
			return []gorums.DialOption{gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(client[id]))}
		}),
	}, extra...)
	systems, stop, err := gorums.NewLocalServers(n, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	t.Cleanup(cancel)
	nodes := make([]*Replica, n)
	deliveries := make([]chan Delivery, n)
	for i, sys := range systems {
		deliveries[i] = make(chan Delivery, 100)
		if nodes[i], err = NewReplica(ctx, sys, f, func(d Delivery) { deliveries[i] <- d }); err != nil {
			t.Fatal(err)
		}
	}
	return nodes, systems, deliveries
}

func ownCert(id uint32) uint32 { return id }

// senderLog records the sender ID of every BRB request a server handles.
type senderLog struct {
	mu      sync.Mutex
	senders map[uint32]map[uint32]bool // server ID -> sender IDs
	count   map[uint32]*atomic.Int32
}

func newSenderLog(n int) *senderLog {
	l := &senderLog{senders: map[uint32]map[uint32]bool{}, count: map[uint32]*atomic.Int32{}}
	for id := uint32(1); id <= uint32(n); id++ {
		l.senders[id] = map[uint32]bool{}
		l.count[id] = new(atomic.Int32)
	}
	return l
}

func (l *senderLog) option() gorums.LocalServerOption {
	return gorums.WithLocalServerOptionsFunc(func(self gorums.ID) []gorums.ServerOption {
		return []gorums.ServerOption{gorums.WithServerInterceptors(func(ctx gorums.ServerContext, msg *gorums.Message, next gorums.Handler) (*gorums.Message, error) {
			if strings.HasPrefix(msg.GetMethod(), "brb.v1.Bracha.") {
				l.mu.Lock()
				l.senders[self][ctx.SenderID()] = true
				l.mu.Unlock()
				l.count[self].Add(1)
			}
			return next(ctx, msg)
		})}
	})
}

func (l *senderLog) seen(server uint32) map[uint32]bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[uint32]bool{}
	for id := range l.senders[server] {
		out[id] = true
	}
	return out
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func TestTLSReplicasDeliverUnderVerifiedIdentities(t *testing.T) {
	creds := generateCreds(t, 4)
	log := newSenderLog(4)
	nodes, systems, deliveries := tlsNodes(t, creds, 4, 1, ownCert, log.option())
	serve(t, systems)
	if err := nodes[0].Broadcast(1, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	for _, ch := range deliveries {
		awaitDelivery(t, ch, BroadcastID{1, 1}, "hello")
	}
	// Every vote arrived under a configured, verified replica ID, and node 1
	// heard from enough distinct replicas to deliver.
	for server := uint32(1); server <= 4; server++ {
		for sender := range log.seen(server) {
			if sender < 1 || sender > 4 {
				t.Fatalf("server %d handled a request from sender %d", server, sender)
			}
		}
	}
	if got := len(log.seen(1)); got < 3 {
		t.Fatalf("node 1 saw votes from %d replicas, want at least 2f+1 = 3", got)
	}
}

// clientCreds builds client TLS credentials that trust the project CA and
// present the certificate in certDir, if any.
func clientCreds(t *testing.T, creds, certDir string) credentials.TransportCredentials {
	t.Helper()
	pem, err := os.ReadFile(filepath.Join(creds, peertls.CAFile))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool}
	if certDir != "" {
		pair, err := tls.LoadX509KeyPair(filepath.Join(certDir, peertls.CertFile), filepath.Join(certDir, peertls.KeyFile))
		if err != nil {
			t.Fatal(err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return credentials.NewTLS(cfg)
}

func TestTLSRefusesUnverifiedClients(t *testing.T) {
	creds := generateCreds(t, 4)
	foreign := generateCreds(t, 4) // a different CA
	log := newSenderLog(4)
	nodes, systems, _ := tlsNodes(t, creds, 4, 1, ownCert, log.option())
	serve(t, systems)
	claim1 := gorums.WithMetadata(metadata.Pairs("gorums-node-id", "1"))
	for i, tc := range []struct {
		name     string
		certDir  string
		claim    bool
		wantCode codes.Code // codes.OK means any refusal
	}{
		// Node 3's valid certificate while claiming node 1's ID.
		{"impostor claims another peer", credDir(creds, 3), true, codes.Unauthenticated},
		{"claim without certificate", "", true, codes.OK},
		{"client without certificate", "", false, codes.OK},
		{"certificate from untrusted CA", credDir(foreign, 1), true, codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := log.count[2].Load()
			opts := []gorums.DialOption{gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(clientCreds(t, creds, tc.certDir)))}
			if tc.claim {
				opts = append(opts, claim1)
			}
			cfg, closeCfg, err := gorums.NewConfig(gorums.WithNodeList([]string{systems[1].Addr()}), opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(closeCfg)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			// A separate ID per case keeps one case's outcome out of the next.
			id := BroadcastID{Origin: 1, Sequence: uint64(10 + i)}
			_ = brbv1.Ready(cfg.Context(ctx), phaseMessage(id, []byte("forged"))).Send()
			if !waitFor(t, 3*time.Second, func() bool { return cfg[0].LastErr() != nil }) {
				t.Fatal("the refused client saw no error")
			}
			if tc.wantCode != codes.OK && status.Code(cfg[0].LastErr()) != tc.wantCode {
				t.Fatalf("LastErr = %v, want code %v", cfg[0].LastErr(), tc.wantCode)
			}
			if n := log.count[2].Load() - before; n != 0 {
				t.Fatalf("server 2 handled %d requests from a refused client", n)
			}
			m := nodes[1].machine
			m.mu.Lock()
			defer m.mu.Unlock()
			if m.instances[id] != nil {
				t.Fatal("a refused client allocated protocol state")
			}
		})
	}
}

// Server 3 presents node 2's certificate. Replicas dialing it refuse the
// stream before sending (dial-side check), and replicas it dials refuse its
// claim to be node 3 (inbound check). The other replicas still deliver.
func TestTLSRefusesServerWithWrongIdentity(t *testing.T) {
	creds := generateCreds(t, 4)
	nodes, systems, deliveries := tlsNodes(t, creds, 4, 1, func(id uint32) uint32 {
		if id == 3 {
			return 2
		}
		return id
	})
	for _, sys := range systems {
		go func() { _ = sys.ListenAndServe() }()
	}
	check := func(from, to uint32, want string) {
		t.Helper()
		node := systems[from-1].PeerConfig().Node(to)
		if node == nil {
			t.Fatalf("server %d has no node %d", from, to)
		}
		ok := waitFor(t, 5*time.Second, func() bool {
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			_ = brbv1.Ready(gorums.Config{node}.Context(ctx), phaseMessage(BroadcastID{9, 1}, nil)).Send()
			return status.Code(node.LastErr()) == codes.Unauthenticated
		})
		if !ok {
			t.Fatalf("server %d to node %d: LastErr = %v, want Unauthenticated", from, to, node.LastErr())
		}
		if want != "" && !strings.Contains(node.LastErr().Error(), want) {
			t.Fatalf("server %d to node %d: %v, want it to mention %q", from, to, node.LastErr(), want)
		}
	}
	check(1, 3, "node 3 failed verification") // dial side: wrong server identity
	check(3, 1, "")                           // inbound: node 1 refuses server 3's claim
	// Replicas 1, 2 and 4 still form a quorum with f = 1.
	if err := nodes[0].Broadcast(1, []byte("hello")); err != nil && !strings.Contains(err.Error(), "send failure") {
		t.Fatal(err)
	}
	for _, i := range []int{0, 1, 3} {
		awaitDelivery(t, deliveries[i], BroadcastID{1, 1}, "hello")
	}
}
