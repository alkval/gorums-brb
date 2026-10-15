package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alkval/gorums-brb/internal/bench"
	"github.com/relab/gorums/benchkit"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := execute(ctx, os.Args[1:], os.Stderr)
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Print(err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, output io.Writer) error {
	mode, args, err := commandMode(args)
	if err != nil {
		return err
	}
	if mode == "bench" {
		opts, err := bench.ParseOptions(args, output)
		if err != nil {
			return err
		}
		benchkit.SetVerbose(opts.Verbose)
		return bench.Run(ctx, opts)
	}
	opts, err := parseOptions(args, output)
	if err != nil {
		return err
	}
	return run(ctx, opts, log.New(output, "", log.LstdFlags))
}

// Sweep appends -mode in extra arguments, so it need not be the first flag.
func commandMode(args []string) (string, []string, error) {
	mode := "node"
	rest := make([]string, 0, len(args))
	seen := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			rest = append(rest, args[i:]...)
			break
		}
		if arg == "-mode" || arg == "--mode" || strings.HasPrefix(arg, "-mode=") || strings.HasPrefix(arg, "--mode=") {
			if seen {
				return "", nil, errors.New("brb: -mode specified more than once")
			}
			seen = true
			_, value, inline := strings.Cut(arg, "=")
			if !inline {
				i++
				if i == len(args) {
					return "", nil, errors.New("brb: -mode needs node or bench")
				}
				value = args[i]
			}
			mode = value
			continue
		}
		rest = append(rest, arg)
		// Preserve values such as -broadcast "-mode=bench". Only these
		// boolean flags may omit a value in the two supported modes.
		name := strings.TrimLeft(arg, "-")
		if strings.HasPrefix(arg, "-") && !strings.Contains(arg, "=") &&
			name != "h" && name != "help" && name != "verbose" && name != "fail-fast" && name != "optimistic" && i+1 < len(args) {
			i++
			rest = append(rest, args[i])
		}
	}
	if mode != "node" && mode != "bench" {
		return "", nil, fmt.Errorf("brb: unknown mode %q, want node or bench", mode)
	}
	return mode, rest, nil
}
