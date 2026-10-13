package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alkval/gorums-brb/internal/brb"
	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
	"github.com/relab/gorums/benchkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type observedFault struct {
	receiver uint32
	phase    string
	id       brb.BroadcastID
	value    string
}

// Observe the messages at the receiving RPC handlers, before the real replica
// processes them. This distinguishes wrong ECHOs from a merely silent peer.
type observingReplica struct {
	*brb.Replica
	id       uint32
	observed chan<- observedFault
}

func (r observingReplica) record(phase string, msg *brbv1.PhaseMessage) {
	if msg.GetSender() == 4 {
		r.observed <- observedFault{r.id, phase,
			brb.BroadcastID{Origin: brb.ProcessID(msg.GetId().GetOrigin()), Sequence: msg.GetId().GetSequence()},
			string(msg.GetValue())}
	}
}

func (r observingReplica) Propose(ctx gorums.ServerContext, msg *brbv1.PhaseMessage) {
	r.record("propose", msg)
	r.Replica.Propose(ctx, msg)
}

func (r observingReplica) Echo(ctx gorums.ServerContext, msg *brbv1.PhaseMessage) {
	r.record("echo", msg)
	r.Replica.Echo(ctx, msg)
}

func TestNetworkFaultScenarios(t *testing.T) {
	for _, mode := range []string{"silent", "wrong-echo", "equivocate"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			systems, stop, err := gorums.NewLocalServers(4, gorums.WithLocalDialOptions(
				gorums.WithGRPCDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { cancel(); stop() }()
			nodes := make([]*brb.Replica, 4)
			observed := make(chan observedFault, 16)
			deliveries := make([]chan brb.Delivery, 4)
			for i, server := range systems {
				deliveries[i] = make(chan brb.Delivery, 4)
				nodes[i], err = brb.NewReplica(ctx, server, 1, func(d brb.Delivery) { deliveries[i] <- d })
				if err != nil {
					t.Fatal(err)
				}
				brbv1.RegisterBrachaServer(server, observingReplica{nodes[i], uint32(i + 1), observed})
			}
			registerFault(ctx, systems[3], mode)
			for _, server := range systems {
				go func() { _ = server.ListenAndServe() }()
			}
			for _, server := range systems {
				if err := server.WaitForPeers(ctx, func(c gorums.Config) bool { return len(c) == 4 }); err != nil {
					t.Fatal(err)
				}
			}
			origin, value := brb.ProcessID(1), "hello"
			if mode == "equivocate" {
				origin, value = 4, "A"
				if err := equivocate(ctx, systems[3]); err != nil {
					t.Fatal(err)
				}
			} else if err := nodes[0].Broadcast(1, []byte(value)); err != nil {
				t.Fatal(err)
			}
			if mode != "silent" {
				phase := "echo"
				want := map[uint32]string{1: "iello", 2: "iello", 3: "iello"}
				if mode == "equivocate" {
					phase = "propose"
					want = map[uint32]string{1: "A", 2: "B", 3: "A"}
				}
				for len(want) > 0 {
					select {
					case event := <-observed:
						if event.phase != phase {
							continue
						}
						expected, ok := want[event.receiver]
						if !ok || event.value != expected || event.id != (brb.BroadcastID{Origin: origin, Sequence: 1}) {
							t.Fatalf("unexpected faulty message: %+v, remaining expectations: %v", event, want)
						}
						delete(want, event.receiver)
					case <-ctx.Done():
						t.Fatalf("%s fault was not observed at replicas %v", mode, want)
					}
				}
			}
			for i := range 3 {
				select {
				case d := <-deliveries[i]:
					if d.ID != (brb.BroadcastID{Origin: origin, Sequence: 1}) || string(d.Value) != value {
						t.Fatalf("node %d: %+v", i+1, d)
					}
				case <-ctx.Done():
					t.Fatalf("node %d did not deliver in %s scenario", i+1, mode)
				}
			}
		})
	}
}

func TestFaultModesWriteOnlyCorrectOriginReports(t *testing.T) {
	for _, mode := range []string{"silent", "wrong-echo", "equivocate"} {
		t.Run(mode, func(t *testing.T) {
			peers := addresses(t, 4)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			paths := make([]string, 4)
			done := make(chan error, 4)
			for i, addr := range peers {
				paths[i] = filepath.Join(t.TempDir(), "result.binpb")
				opts, err := ParseOptions([]string{"-self=" + addr, "-remotes=" + strings.Join(peers, ","), "-fault=" + mode,
					"-output=" + paths[i], "-time=100ms", "-payload=256", "-rate=50"}, io.Discard)
				if err != nil {
					t.Fatal(err)
				}
				go func() { done <- Run(ctx, opts) }()
			}
			for range 4 {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			for i, path := range paths[:3] {
				checkReport(t, path, 4, 1, 256)
				report, err := benchkit.LoadReport(path)
				if err != nil {
					t.Fatal(err)
				}
				want := fmt.Sprintf("node %d f=1 fail-fast=true fault=%s fault-node=4 kill-after=0s", i+1, mode)
				if report.GetLabel() != want {
					t.Fatalf("scenario not recorded: %s", report.GetLabel())
				}
			}
			if _, err := os.Stat(paths[3]); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("faulty origin wrote a measurement: %v", err)
			}
		})
	}
}

func TestFaultOptionValidation(t *testing.T) {
	base := []string{"-self=127.0.0.1:9001", "-remotes=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004", "-output=unused", "-time=2s"}
	for _, flag := range []string{"-fault=silent", "-fault=wrong-echo", "-fault=equivocate", "-fault-kill-after=1s"} {
		opts, err := ParseOptions(append(append([]string{}, base...), flag), io.Discard)
		if err != nil || !opts.failFast || opts.f != 1 || opts.faultNode != 4 {
			t.Fatalf("%s: %+v, %v", flag, opts, err)
		}
	}
	for _, flag := range []string{"-fault=invalid", "-fault-node=5", "-fault-kill-after=-1s", "-fault-kill-after=3s"} {
		if _, err := ParseOptions(append(append([]string{}, base...), flag), io.Discard); err == nil {
			t.Fatalf("accepted %s", flag)
		}
	}
}
