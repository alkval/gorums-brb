package bench

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/relab/gorums"
)

func TestStartupWaitsForEveryReplica(t *testing.T) {
	for _, optimistic := range []bool{false, true} {
		for _, failFast := range []bool{false, true} {
			t.Run(fmt.Sprintf("optimistic=%t/fail-fast=%t", optimistic, failFast), func(t *testing.T) {
				peers := addresses(t, 7)
				ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
				defer cancel()
				nodes := make([]*benchmarkNode, len(peers))
				opts := make([]Options, len(peers))
				// Cancel waiting handlers before stopping servers, including when
				// the test fails while node 3 is held at its startup gate.
				defer func() {
					cancel()
					for _, node := range nodes {
						if node != nil {
							node.server.Stop()
						}
					}
				}()
				for i, peer := range peers {
					var err error
					opts[i], err = ParseOptions([]string{
						"-self=" + peer, "-remotes=" + strings.Join(peers, ","),
						"-f=2", "-output=" + filepath.Join(t.TempDir(), "result.binpb"),
						fmt.Sprintf("-optimistic=%t", optimistic), fmt.Sprintf("-fail-fast=%t", failFast),
					}, io.Discard)
					if err != nil {
						t.Fatal(err)
					}
					nodes[i], err = startNode(ctx, opts[i], nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				// Every server is connected, but node 3 has not run its startup
				// checks. Connections alone must not allow measurement to start.
				for _, node := range nodes {
					if err := node.server.WaitForPeers(ctx, func(c gorums.Config) bool { return len(c) == len(peers) }); err != nil {
						t.Fatal(err)
					}
				}
				started := make(chan error, len(peers))
				for i, node := range nodes {
					if i == 2 {
						continue
					}
					go func() { started <- awaitPeers(ctx, node, opts[i]) }()
				}
				for i, node := range nodes {
					if i == 2 {
						continue
					}
					select {
					case <-node.probed:
					case <-ctx.Done():
						t.Fatal("peers did not complete probes")
					}
				}
				select {
				case err := <-started:
					t.Fatalf("startup completed before node 3 was ready: %v", err)
				case <-time.After(100 * time.Millisecond):
				}
				for _, node := range nodes {
					select {
					case <-node.ready:
						t.Fatal("protocol gate opened before the group was ready")
					default:
					}
				}
				// Release the delayed replica. Every peer must now finish startup.
				go func() { started <- awaitPeers(ctx, nodes[2], opts[2]) }()
				for range nodes {
					select {
					case err := <-started:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal("group startup did not finish")
					}
				}
				for _, node := range nodes {
					select {
					case <-node.ready:
					default:
						t.Fatal("startup returned with a closed protocol gate")
					}
				}
			})
		}
	}
}
