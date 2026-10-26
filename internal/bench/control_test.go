package bench

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/relab/gorums"
	"github.com/relab/gorums/benchkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestControlMessagesBindToSender(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ready := make(chan struct{})
	close(ready)
	dial := gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	systems, stop, err := gorums.NewLocalServers(2, gorums.WithLocalDialOptions(dial),
		gorums.WithLocalServerOptions(gorums.WithServerInterceptors(serverInterceptor(ctx, ready, 2))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	control := benchkit.NewControl()
	done := control.ArmDone(2)
	benchkit.RegisterControlServer(systems[0], readyControl{Control: control, probed: ready})
	benchkit.RegisterControlServer(systems[1], benchkit.NewControl())
	for _, sys := range systems {
		go func() { _ = sys.ListenAndServe() }()
	}
	for _, sys := range systems {
		if err := sys.WaitForPeers(ctx, func(c gorums.Config) bool { return len(c) == 2 }); err != nil {
			t.Fatal(err)
		}
	}
	missing := func(want ...uint32) bool { return slices.Equal(control.MissingDone(), want) }
	waitFor := func(want ...uint32) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !missing(want...) {
			if time.Now().After(deadline) {
				t.Fatalf("missing Done = %v, want %v", control.MissingDone(), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// An anonymous client cannot signal completion or use other control calls.
	// ClockSync on the same stream returns after the Done was handled.
	anon, closeAnon, err := gorums.NewConfig(gorums.WithNodeList([]string{systems[0].Addr()}), dial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeAnon)
	_ = benchkit.Done(anon.Context(ctx), benchkit.DoneRequest_builder{SenderId: 2}.Build()).Send()
	if _, err := benchkit.ClockSync(anon.Context(ctx), benchkit.ClockSyncRequest_builder{}.Build()).All(); err == nil ||
		!strings.Contains(err.Error(), "non-member") {
		t.Fatalf("anonymous ClockSync = %v, want a membership refusal", err)
	}
	if !missing(1, 2) {
		t.Fatalf("anonymous Done was counted: missing %v", control.MissingDone())
	}

	// Node 2 claims to be node 1 but is counted only as itself.
	toNode1 := gorums.Config{systems[1].PeerConfig().Node(1)}
	if err := benchkit.Done(toNode1.Context(ctx), benchkit.DoneRequest_builder{SenderId: 1}.Build()).Send(); err != nil {
		t.Fatal(err)
	}
	waitFor(1)

	// Node 1's own signal arrives in-process under its own ID.
	self := gorums.Config{systems[0].PeerConfig().Node(1)}
	if err := benchkit.Done(self.Context(ctx), benchkit.DoneRequest_builder{SenderId: 2}.Build()).Send(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("completion not reached: missing %v", control.MissingDone())
	}
}
