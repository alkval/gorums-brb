package main

import (
	"errors"
	"flag"
	"io"
	"testing"
)

func TestModeSelection(t *testing.T) {
	mode, rest, err := commandMode([]string{"-broadcast", "-mode=bench"})
	if err != nil || mode != "node" || len(rest) != 2 || rest[1] != "-mode=bench" {
		t.Fatalf("payload parsed as a mode: %s %v %v", mode, rest, err)
	}
	for _, args := range [][]string{{"-mode=bench", "-h"}, {"-h", "-mode", "bench"}, {"-mode=node", "-h"}, {"-h"}} {
		if err := execute(t.Context(), args, io.Discard); !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("%v: %v", args, err)
		}
	}
	mode, _, err = commandMode([]string{"-optimistic", "-mode=bench"})
	if err != nil || mode != "bench" {
		t.Fatalf("optimistic flag swallowed mode: %s %v", mode, err)
	}
	for _, args := range [][]string{{"-mode"}, {"-mode=bad"}, {"-mode=node", "-mode=bench"}, {"-mode=node", "-workers=1"}, {"-mode=bench", "-broadcast=hello"}} {
		if err := execute(t.Context(), args, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
