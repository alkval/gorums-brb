package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestFourProcessesDeliver(t *testing.T) {
	testFourProcessesDeliver(t, []int{1, 2, 3, 4}, 0, []senderWorkload{{count: 1}})
}

func TestFourProcessesDeliverWithLateOrigin(t *testing.T) {
	testFourProcessesDeliver(t, []int{2, 3, 4, 1}, 2*time.Second, []senderWorkload{{count: 1}})
}

func TestFourProcessesDeliverRepeatedBroadcasts(t *testing.T) {
	testFourProcessesDeliver(t, []int{1, 2, 3, 4}, 0, []senderWorkload{{count: 3}})
}

type senderWorkload struct {
	count int
}

// The workloads belong to consecutive sender IDs, starting at 1.
func testFourProcessesDeliver(t *testing.T, order []int, originDelay time.Duration, workloads []senderWorkload) {
	t.Helper()
	if testing.Short() {
		t.Skip("separate-process smoke test")
	}
	tmp := t.TempDir()
	binary := filepath.Join(tmp, "brb")
	buildCtx, buildCancel := context.WithTimeout(t.Context(), time.Minute)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build brb: %v\n%s", err, output)
	}

	// Hold all four ports together so the selected addresses are distinct.
	var listeners []net.Listener
	t.Cleanup(func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	})
	var addresses []string
	for range 4 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}
	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}

	type process struct {
		id      int
		cmd     *exec.Cmd
		done    chan struct{}
		err     error // read only after done closes
		logPath string
	}
	var processes []*process
	expectedDeliveries := make(map[string]int)
	for sender, workload := range workloads {
		for i := range workload.count {
			value := `value="hello"`
			expectedDeliveries[fmt.Sprintf("origin=%d sequence=%d %s", sender+1, i+1, value)] = 0
		}
	}
	checkDeliveries := false
	t.Cleanup(func() {
		// Signal everyone before waiting, with a forced-stop fallback on failure.
		for _, p := range processes {
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		for _, p := range processes {
			select {
			case <-p.done:
				if p.err != nil {
					t.Errorf("node %d exit: %v", p.id, p.err)
				}
			case <-time.After(5 * time.Second):
				_ = p.cmd.Process.Kill()
				<-p.done
				t.Errorf("node %d did not stop after SIGTERM", p.id)
			}
			output, err := os.ReadFile(p.logPath)
			if err != nil {
				t.Errorf("node %d log: %v", p.id, err)
				continue
			}
			t.Logf("node %d output:\n%s", p.id, output)
			if !checkDeliveries {
				continue
			}
			expected := make(map[string]int)
			for delivery := range expectedDeliveries {
				expected[delivery] = 0
			}
			for line := range strings.SplitSeq(string(output), "\n") {
				if !strings.Contains(line, " delivered ") {
					continue
				}
				_, delivery, found := strings.Cut(line, fmt.Sprintf("node %d delivered ", p.id))
				if _, valid := expected[delivery]; !found || !valid {
					t.Errorf("node %d unexpected delivery: %s", p.id, line)
					continue
				}
				expected[delivery]++
			}
			for delivery, occurrences := range expected {
				if occurrences != 1 {
					t.Errorf("node %d logged %d deliveries of %s, want 1", p.id, occurrences, delivery)
				}
			}
		}
	})

	peers := strings.Join(addresses, ",")
	var lastStarted time.Time
	for _, id := range order {
		if id == 1 && originDelay > 0 {
			// Let receivers retry connections while the origin is still missing.
			time.Sleep(originDelay)
		}
		logPath := filepath.Join(tmp, fmt.Sprintf("node%d.log", id))
		output, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		args := []string{"-id", strconv.Itoa(id), "-f", "1", "-peers", peers}
		if id <= len(workloads) {
			workload := workloads[id-1]
			args = append(args, "-broadcast", "hello")
			args = append(args, "-count", strconv.Itoa(workload.count))
		}
		cmd := exec.Command(binary, args...)
		cmd.Stdout, cmd.Stderr = output, output
		err = cmd.Start()
		_ = output.Close() // the child owns its duplicated file descriptors
		if err != nil {
			t.Fatalf("start node %d: %v", id, err)
		}
		p := &process{id: id, cmd: cmd, done: make(chan struct{}), logPath: logPath}
		go func() {
			p.err = p.cmd.Wait()
			close(p.done)
		}()
		processes = append(processes, p)
		lastStarted = time.Now()
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		allDelivered := true
		for _, p := range processes {
			select {
			case <-p.done:
				t.Fatalf("node %d exited before the demo finished: %v", p.id, p.err)
			default:
			}
			output, err := os.ReadFile(p.logPath)
			if err != nil {
				t.Fatal(err)
			}
			for delivery := range expectedDeliveries {
				expected := fmt.Sprintf("node %d delivered %s", p.id, delivery)
				allDelivered = allDelivered && strings.Contains(string(output), expected)
			}
		}
		if allDelivered {
			checkDeliveries = true
			t.Logf("all four delivered %d broadcasts %s after the last process started", len(expectedDeliveries), time.Since(lastStarted))
			return // cleanup checks the final logs and each process's exit status
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for all broadcasts at all four processes")
		case <-ticker.C:
		}
	}
}
