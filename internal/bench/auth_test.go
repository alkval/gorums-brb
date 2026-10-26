package bench

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alkval/gorums-brb/internal/peertls"
	"github.com/relab/gorums/benchkit"
)

func benchCreds(t *testing.T, peers []string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "creds")
	if err := peertls.Generate(dir, peers, time.Hour); err != nil {
		t.Fatal(err)
	}
	return dir
}

func nodeCreds(creds string, id int) string {
	return filepath.Join(creds, "node-"+strconv.Itoa(id))
}

func TestBenchmarkRequiresOneTransport(t *testing.T) {
	base := []string{"-self=127.0.0.1:9000", "-remotes=127.0.0.1:9000", "-f=0", "-output=result.binpb"}
	if _, err := ParseOptions(base, io.Discard); err == nil {
		t.Fatal("accepted a benchmark without -tls-dir or -plaintext")
	}
	opts, err := ParseOptions(append(append([]string{}, base...), "-tls-dir=creds"), io.Discard)
	if err != nil || opts.auth() != "tls" {
		t.Fatalf("TLS options: %+v, %v", opts, err)
	}
	if _, err := ParseOptions(append(append([]string{}, base...), "-tls-dir=creds", "-plaintext"), io.Discard); err == nil {
		t.Fatal("accepted both -tls-dir and -plaintext")
	}
}

func TestAuthenticatedBenchmarkProducesReports(t *testing.T) {
	peers := addresses(t, 4)
	creds := benchCreds(t, peers)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 4)
	paths := make([]string, 4)
	for i, peer := range peers {
		paths[i] = filepath.Join(t.TempDir(), "result.binpb")
		opts, err := ParseOptions([]string{"-tls-dir=" + nodeCreds(creds, i+1), "-self=" + peer,
			"-remotes=" + strings.Join(peers, ","), "-f=1", "-output=" + paths[i], "-time=200ms", "-payload=256"}, io.Discard)
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
	for i, path := range paths {
		checkReport(t, path, 4, 1, 256)
		report, err := benchkit.LoadReport(path)
		if err != nil {
			t.Fatal(err)
		}
		if want := "node " + strconv.Itoa(i+1) + " f=1 auth=tls"; report.GetLabel() != want {
			t.Fatalf("label %q, want %q", report.GetLabel(), want)
		}
	}
}

// Credentials for another replica must stop the run at startup: before the
// listener opens, so no peer can reach it, and without writing a report.
func TestBenchmarkRejectsWrongCredentials(t *testing.T) {
	peers := addresses(t, 4)
	creds := benchCreds(t, peers)
	path := filepath.Join(t.TempDir(), "result.binpb")
	opts, err := ParseOptions([]string{"-tls-dir=" + nodeCreds(creds, 2), "-self=" + peers[0],
		"-remotes=" + strings.Join(peers, ","), "-f=1", "-output=" + path, "-time=200ms"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := Run(ctx, opts); err == nil || !strings.Contains(err.Error(), "does not name") {
		t.Fatalf("Run with node 2's credentials = %v, want an identity error", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("a failed TLS setup wrote a report: %v", err)
	}
	listener, err := net.Listen("tcp", peers[0])
	if err != nil {
		t.Fatalf("a failed TLS setup left its listener open: %v", err)
	}
	_ = listener.Close()
}
