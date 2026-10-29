//go:build linux

package bench

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"
)

// This runs on Linux CI as well as any later approved Linux capability check.
func TestNetRealProcFiles(t *testing.T) {
	for _, name := range []string{"tcp", "tcp6", "snmp", "netstat"} {
		data, err := readNetProc(t.Context(), name)
		if err != nil {
			t.Fatal(name, err)
		}
		switch name {
		case "tcp", "tcp6":
			_, _, err = parseSockets(data, nativeLittleEndian())
		case "snmp":
			_, err = parseCounters(data, "Tcp", []string{"RetransSegs", "OutSegs"})
		case "netstat":
			_, err = parseCounters(data, "TcpExt", []string{"TCPTimeouts"})
		}
		if err != nil {
			t.Fatal(name, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := readNetProc(ctx, "tcp"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestNetLogSinkSupportsPipeSocketAndFile(t *testing.T) {
	for _, kind := range []string{"pipe", "socketpair", "file"} {
		t.Run(kind, func(t *testing.T) {
			var w, r *os.File
			switch kind {
			case "pipe":
				var err error
				r, w, err = os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
			case "socketpair":
				fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
				if err != nil {
					t.Fatal(err)
				}
				w, r = os.NewFile(uintptr(fds[0]), "writer"), os.NewFile(uintptr(fds[1]), "reader")
			case "file":
				var err error
				w, err = os.CreateTemp(t.TempDir(), "log")
				if err != nil {
					t.Fatal(err)
				}
			}
			defer w.Close()
			if r != nil {
				defer r.Close()
			}
			original := os.Stderr
			os.Stderr = w
			defer func() { os.Stderr = original }()
			flags := func() uintptr {
				raw, err := w.SyscallConn()
				if err != nil {
					t.Fatal(err)
				}
				var value uintptr
				if err := raw.Control(func(fd uintptr) {
					var errno syscall.Errno
					value, _, errno = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
					if errno != 0 {
						t.Fatal(errno)
					}
				}); err != nil {
					t.Fatal(err)
				}
				return value
			}
			before := flags()
			c, err := newNetCollector(1, []string{"192.0.2.1:9000"}, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.stop()
			if _, err := w.WriteString("before\n"); err != nil {
				t.Fatal(err)
			}
			if err := c.write([]byte("network\n")); err != nil {
				t.Fatal(err)
			}
			if err := c.stop(); err != nil {
				t.Fatal(err)
			}
			if _, err := w.WriteString("after\n"); err != nil {
				t.Fatal("collector closed stderr", err)
			}
			after := flags()
			if before != after {
				t.Fatal("stderr flags changed", before, after)
			}
			want := "before\nnetwork\nafter\n"
			got := make([]byte, len(want))
			if kind == "file" {
				if _, err := w.ReadAt(got, 0); err != nil {
					t.Fatal(err)
				}
			} else if _, err := io.ReadFull(r, got); err != nil {
				t.Fatal(err)
			}
			if string(got) != want {
				t.Fatalf("sink lost or overwrote records: %q", got)
			}
		})
	}
}

func TestNetLogSinkUnavailableIsDiagnosticFailure(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "closed-log")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	original := os.Stderr
	os.Stderr = f
	defer func() { os.Stderr = original }()
	c, err := newNetCollector(1, []string{"192.0.2.1:9000"}, time.Second)
	if err != nil {
		t.Fatal("sink failure rejected benchmark setup", err)
	}
	defer c.stop()
	if err := c.write([]byte("network\n")); err == nil {
		t.Fatal("closed sink accepted write")
	}
	c.stop()
	if c.shutdownStatus != "not_started" {
		t.Fatal(c.shutdownStatus)
	}
}
