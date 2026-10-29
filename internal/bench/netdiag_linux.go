//go:build linux

package bench

import (
	"context"
	"io"
	"os"
	"syscall"
	"time"
)

const netDiagSupported = true

func newNetCollector(self uint32, peers []string, interval time.Duration) (*netCollector, error) {
	// Dup shares stderr's offset and flags for pipes, socketpairs and files.
	// Raw writes avoid taking os.Stderr's Go write mutex. The worker owns the
	// duplicate until it exits, including after a timed-out join.
	// Fd() can put a Go-managed pipe into blocking mode. Control obtains the
	// descriptor without changing it and keeps it live during Dup.
	fd := -1
	raw, sinkErr := os.Stderr.SyscallConn()
	if sinkErr == nil {
		controlErr := raw.Control(func(original uintptr) {
			fd, sinkErr = syscall.Dup(int(original))
			if sinkErr == nil {
				syscall.CloseOnExec(fd)
			}
		})
		if controlErr != nil {
			sinkErr = controlErr
		}
	}
	write := func(line []byte) error {
		if sinkErr != nil {
			return sinkErr
		}
		n, err := syscall.Write(fd, line)
		if err != nil {
			return err
		}
		if n != len(line) {
			return io.ErrShortWrite
		}
		return nil
	}
	c, err := makeNetCollector(self, peers, interval, readNetProc, write, func() {
		if fd >= 0 {
			_ = syscall.Close(fd)
		}
	})
	if err != nil && fd >= 0 {
		_ = syscall.Close(fd)
	}
	if c != nil {
		c.sinkUnavailable = sinkErr != nil
	}
	return c, err
}

func readNetProc(ctx context.Context, name string) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f, err := os.Open("/proc/net/" + name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, netFileBytes+1))
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return data, err
}
