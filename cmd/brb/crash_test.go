package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/relab/gorums/benchkit"
)

func TestBenchmarkCrashFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("separate-process crash test")
	}
	tmp := t.TempDir()
	binary := filepath.Join(tmp, "brb")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	var peers []string
	var listeners []net.Listener
	for range 4 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		listeners = append(listeners, l)
		peers = append(peers, l.Addr().String())
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	done := make(chan error, 4)
	for i, addr := range peers {
		path := filepath.Join(tmp, fmt.Sprintf("node%d.binpb", i+1))
		cmd := exec.CommandContext(ctx, binary, "-mode=bench", "-self="+addr, "-remotes="+strings.Join(peers, ","), "-output="+path,
			"-time=600ms", "-rate=20", "-payload=256", "-fault-kill-after=200ms", "-fault-node=4")
		logfile, err := os.Create(filepath.Join(tmp, fmt.Sprintf("node%d.log", i+1)))
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stdout, cmd.Stderr = logfile, logfile
		if err := cmd.Start(); err != nil {
			_ = logfile.Close()
			t.Fatal(err)
		}
		_ = logfile.Close()
		go func() { done <- cmd.Wait() }()
	}
	for range 4 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	for id := 1; id <= 3; id++ {
		logs, err := os.ReadFile(filepath.Join(tmp, fmt.Sprintf("node%d.log", id)))
		if err != nil {
			t.Fatal(err)
		}
		for _, message := range []string{"first ECHO multicast failure", "first READY multicast failure", "multicast failures: ECHO="} {
			if got := strings.Count(string(logs), message); got != 1 {
				t.Fatalf("survivor %d: %q appeared %d times, want 1\n%s", id, message, got, logs)
			}
		}
		report, err := benchkit.LoadReport(filepath.Join(tmp, fmt.Sprintf("node%d.binpb", id)))
		if err != nil {
			t.Fatal(err)
		}
		if len(report.GetResults()) != 1 || report.GetResults()[0].GetTotalOps() < 2 || report.GetResults()[0].GetFailedOps() != 0 ||
			!strings.Contains(report.GetLabel(), "missing-done=[4]") || !strings.Contains(report.GetLabel(), "kill-after=200ms") {
			t.Fatalf("survivor %d: %v", id, report)
		}
	}
	if _, err := os.Stat(filepath.Join(tmp, "node4.binpb")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("crashed peer wrote a report: %v", err)
	}
}
