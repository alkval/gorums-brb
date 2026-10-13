package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
	"github.com/relab/gorums/benchkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestParseOptions(t *testing.T) {
	base := []string{"-self=127.0.0.1:9000", "-remotes=127.0.0.1:9000", "-f=0", "-output=result.binpb"}
	opts, err := ParseOptions(base, io.Discard)
	if err != nil || opts.selfID != 1 || opts.failFast {
		t.Fatalf("valid Options: %+v, %v", opts, err)
	}
	for _, extra := range []string{
		"-workers=0", "-payload=-1", "-payload=1048577", "-time=0s", "-rate=-1",
		"-f=1", "-output=", "-self=0.0.0.0:9000", "-self=127.0.0.1:0",
		"-self=127.0.0.1:9001", "-remotes=127.0.0.1:9000,127.0.0.1:9000",
		"-benchmarks=Other", "-stream-mode=dedup", "-cpuprofile=cpu.out",
		"-rate-step=1", "-call-timeout=1s", "-fault-kill-after=1s", "extra",
	} {
		t.Run(extra, func(t *testing.T) {
			if _, err := ParseOptions(append(append([]string{}, base...), extra), io.Discard); err == nil {
				t.Fatal("accepted invalid or unsupported Options")
			}
		})
	}
}

func addresses(t *testing.T, count int) []string {
	t.Helper()
	var listeners []net.Listener
	var result []string
	for range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		t.Cleanup(func() { _ = listener.Close() })
		result = append(result, listener.Addr().String())
	}
	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func checkReport(t *testing.T, path string, nodes, workers, payload int) {
	t.Helper()
	report, err := benchkit.LoadReport(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.GetResults()) != 1 {
		t.Fatalf("got %d results, want 1", len(report.GetResults()))
	}
	r := report.GetResults()[0]
	c := r.GetConfig()
	if c.GetName() != benchmarkName || int(c.GetNumNodes()) != nodes || int(c.GetWorkers()) != workers || int(c.GetPayload()) != payload {
		t.Fatalf("incorrect report metadata: %v", c)
	}
	if r.GetTotalOps() == 0 || r.GetFailedOps() != 0 || r.GetThroughput() <= 0 || len(r.Percentiles(0.95)) != 1 {
		t.Fatalf("invalid measurement: ops=%d failed=%d throughput=%f", r.GetTotalOps(), r.GetFailedOps(), r.GetThroughput())
	}
}

func TestLocalDeliveryAndReport(t *testing.T) {
	for _, size := range []int{0, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			peer := addresses(t, 1)[0]
			path := filepath.Join(t.TempDir(), "result.binpb")
			opts, err := ParseOptions([]string{"-self=" + peer, "-remotes=" + peer, "-f=0",
				"-output=" + path, "-time=50ms", "-workers=3", fmt.Sprintf("-payload=%d", size)}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := Run(ctx, opts); err != nil {
				t.Fatal(err)
			}
			checkReport(t, path, 1, 3, size)
			listener, err := net.Listen("tcp", peer)
			if err != nil {
				t.Fatalf("listener leaked: %v", err)
			}
			_ = listener.Close()
		})
	}
}

func TestFixedRateStopsAtMeasurementDeadline(t *testing.T) {
	peer := addresses(t, 1)[0]
	path := filepath.Join(t.TempDir(), "result.binpb")
	opts, err := ParseOptions([]string{"-self=" + peer, "-remotes=" + peer,
		"-f=0", "-output=" + path, "-time=200ms", "-workers=1", "-rate=1"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := Run(ctx, opts); err != nil {
		t.Fatal(err)
	}
	checkReport(t, path, 1, 1, 0)
	report, err := benchkit.LoadReport(path)
	if err != nil {
		t.Fatal(err)
	}
	r := report.GetResults()[0]
	// The first operation is immediate. The next slot is at one second,
	// outside the 200 ms window, and must not extend the measurement.
	if r.GetTotalOps() != 1 {
		t.Fatalf("completed %d operations, want only the initial operation", r.GetTotalOps())
	}
	if elapsed := time.Duration(r.GetTotalTime()); elapsed < opts.Duration || elapsed >= 750*time.Millisecond {
		t.Fatalf("measurement lasted %s, want about %s, not the next pacing slot", elapsed, opts.Duration)
	}
}

func TestFourNodesProduceReports(t *testing.T) {
	testFourNodesProduceReports(t, false)
}

func TestFailFastWithLateOrigin(t *testing.T) {
	testFourNodesProduceReports(t, true)
}

func testFourNodesProduceReports(t *testing.T, failFast bool) {
	t.Helper()
	peers := addresses(t, 4)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	done := make(chan error, 4)
	paths := make([]string, 4)
	order := []int{0, 1, 2, 3}
	if failFast {
		order = []int{1, 2, 3, 0}
	}
	for _, i := range order {
		if failFast && i == 0 {
			// Let the other nodes attempt connections before origin 1 starts.
			timer := time.NewTimer(300 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				t.Fatal("startup timed out")
			}
		}
		peer := peers[i]
		paths[i] = filepath.Join(t.TempDir(), "result.binpb")
		args := []string{"-self=" + peer, "-remotes=" + strings.Join(peers, ","),
			"-f=1", "-output=" + paths[i], "-time=200ms", "-workers=2", "-payload=256"}
		if failFast {
			args = append(args, "-fail-fast")
		}
		opts, err := ParseOptions(args, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		go func() { done <- Run(ctx, opts) }()
	}
	for range 4 {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	if !t.Failed() {
		for i, path := range paths {
			checkReport(t, path, 4, 2, 256)
			report, err := benchkit.LoadReport(path)
			if err != nil {
				t.Fatal(err)
			}
			label := fmt.Sprintf("node %d f=1", i+1)
			if failFast {
				label += " fail-fast=true"
			}
			if report.GetLabel() != label {
				t.Fatalf("healthy run recorded unexpected faults: %s", report.GetLabel())
			}
		}
	}
}

func TestCancellationWithMissingPeer(t *testing.T) {
	peers := addresses(t, 2)
	opts, err := ParseOptions([]string{"-self=" + peers[0], "-remotes=" + strings.Join(peers, ","),
		"-f=0", "-output=" + filepath.Join(t.TempDir(), "result.binpb")}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := Run(ctx, opts); err == nil {
		t.Fatal("missing-peer startup reported success")
	}
}

func TestPeerCompletionWithCrashedPeer(t *testing.T) {
	systems, stop, err := gorums.NewLocalServers(4, gorums.WithLocalDialOptions(
		gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	controls := make([]*benchkit.Control, 4)
	done := make([]<-chan struct{}, 4)
	for i, srv := range systems {
		controls[i] = benchkit.NewControl()
		done[i] = controls[i].ArmDone(4)
		benchkit.RegisterControlServer(srv, controls[i])
		go func() { _ = srv.ListenAndServe() }()
	}
	for _, srv := range systems {
		if err := srv.WaitForPeers(ctx, func(c gorums.Config) bool { return len(c) == 4 }); err != nil {
			t.Fatal(err)
		}
	}
	systems[3].Stop()
	results := make(chan error, 3)
	for i, srv := range systems[:3] {
		go func() {
			missing, err := finishPeers(ctx, srv, uint32(i+1), controls[i], done[i], 200*time.Millisecond)
			if err == nil && (len(missing) != 1 || missing[0] != 4) {
				err = fmt.Errorf("node %d: missing signals %v, want [4]", i+1, missing)
			}
			results <- err
		}()
	}
	for range 3 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	if _, err := finishPeers(ctx, systems[0], 1, controls[0], done[0], time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled completion returned %v, want context cancellation", err)
	}
}

// Start every benchmark origin before stopping the fourth server. It accepts
// phase messages but sends no votes, so only the three correct replicas deliver.
type quietBenchmarkPeer struct{ started chan<- uint32 }

func (p quietBenchmarkPeer) Propose(_ gorums.ServerContext, msg *brbv1.PhaseMessage) {
	if msg.GetId().GetSequence() == 1 {
		p.started <- msg.GetSender()
	}
}
func (quietBenchmarkPeer) Echo(gorums.ServerContext, *brbv1.PhaseMessage)  {}
func (quietBenchmarkPeer) Ready(gorums.ServerContext, *brbv1.PhaseMessage) {}

func TestBenchmarkWritesReportsAfterPeerCrash(t *testing.T) {
	peers := addresses(t, 4)
	listener, err := net.Listen("tcp", peers[3])
	if err != nil {
		t.Fatal(err)
	}
	peer := gorums.NewServer(gorums.WithPeers(4, gorums.WithNodeList(peers),
		gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	t.Cleanup(peer.Stop)
	started := make(chan uint32, 3)
	brbv1.RegisterBrachaServer(peer, quietBenchmarkPeer{started: started})
	benchkit.RegisterControlServer(peer, benchkit.NewControl())
	go func() { _ = peer.Serve(listener) }()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	results := make(chan error, 3)
	paths := make([]string, 3)
	for i, addr := range peers[:3] {
		paths[i] = filepath.Join(t.TempDir(), "result.binpb")
		opts, err := ParseOptions([]string{"-self=" + addr, "-remotes=" + strings.Join(peers, ","),
			"-f=1", "-fail-fast", "-output=" + paths[i], "-time=300ms", "-rate=20", "-payload=256"}, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		go func() { results <- Run(ctx, opts) }()
	}
	seen := make(map[uint32]bool)
	for len(seen) != 3 {
		select {
		case id := <-started:
			seen[id] = true
		case <-ctx.Done():
			t.Fatal("benchmark origins did not start")
		}
	}
	peer.Stop()
	for range 3 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("benchmark did not finish after the peer crash")
		}
	}
	for _, path := range paths {
		checkReport(t, path, 4, 1, 256)
		report, err := benchkit.LoadReport(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(report.GetLabel(), "fail-fast=true") || !strings.Contains(report.GetLabel(), "missing-done=[4]") ||
			!strings.Contains(report.GetLabel(), "initial-send-failures=") || strings.Contains(report.GetLabel(), "initial-send-failures=0") ||
			report.GetResults()[0].GetTotalOps() < 2 {
			t.Fatalf("report did not record continued delivery and missing completion: %v", report)
		}
	}
}

func TestBenchmarkRequiresDeliveryBeforeReporting(t *testing.T) {
	// With n=2 and f=0, the origin alone cannot reach the ECHO threshold.
	// The other peer accepts every PROPOSE but contributes no protocol votes.
	peers := addresses(t, 2)
	listener, err := net.Listen("tcp", peers[1])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	peer := gorums.NewServer(gorums.WithPeers(2, gorums.WithNodeList(peers),
		gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))))
	t.Cleanup(peer.Stop)
	started := make(chan uint32, 1)
	brbv1.RegisterBrachaServer(peer, quietBenchmarkPeer{started: started})
	benchkit.RegisterControlServer(peer, benchkit.NewControl())
	go func() { _ = peer.Serve(listener) }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "result.binpb")
	opts, err := ParseOptions([]string{"-self=" + peers[0], "-remotes=" + strings.Join(peers, ","),
		"-f=0", "-fail-fast", "-output=" + path, "-time=50ms", "-rate=1"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- Run(ctx, opts) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("origin did not send")
	}
	// Supply the peer's completion signal so the exit barrier cannot hide a
	// bug that counts PROPOSE as delivery. Done contributes no BRB votes.
	if err := benchkit.Done(peer.PeerConfig().Context(ctx), benchkit.DoneRequest_builder{SenderId: 2}.Build()).Send(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		t.Fatalf("benchmark finished without delivery: %v", err)
	case <-time.After(1500 * time.Millisecond): // includes the one-second exit grace
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled, undelivered broadcast reported success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("benchmark did not stop after cancellation")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("undelivered run left a report: %v", err)
	}
}
