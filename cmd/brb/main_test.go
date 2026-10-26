package main

import (
	"errors"
	"flag"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/alkval/gorums-brb/internal/peertls"
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
	for _, flag := range []string{"-optimistic", "-plaintext", "-ready-digest"} {
		mode, _, err = commandMode([]string{flag, "-mode=bench"})
		if err != nil || mode != "bench" {
			t.Fatalf("%s flag swallowed mode: %s %v", flag, mode, err)
		}
	}
	for _, args := range [][]string{{"-mode"}, {"-mode=bad"}, {"-mode=node", "-mode=bench"}, {"-mode=node", "-workers=1"}, {"-mode=bench", "-broadcast=hello"}} {
		if err := execute(t.Context(), args, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestCertsMode(t *testing.T) {
	out := filepath.Join(t.TempDir(), "creds")
	var log strings.Builder
	if err := execute(t.Context(), []string{"-mode=certs", "-out=" + out, "-peers=127.0.0.1:7001,127.0.0.1:7002"}, &log); err != nil {
		t.Fatal(err)
	}
	for id := uint32(1); id <= 2; id++ {
		if _, _, err := peertls.Load(filepath.Join(out, "node-"+strconv.Itoa(int(id))), id); err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
	}
	if !strings.Contains(log.String(), "keep ca-key.pem on this machine") {
		t.Fatalf("missing provisioning guidance: %q", log.String())
	}
	for _, args := range [][]string{
		{"-mode=certs", "-peers=127.0.0.1:7001"},                                   // no -out
		{"-mode=certs", "-out=" + filepath.Join(t.TempDir(), "x")},                 // no -peers
		{"-mode=certs", "-out=" + out, "-peers=127.0.0.1:7001"},                    // existing output
		{"-mode=certs", "-out=" + filepath.Join(t.TempDir(), "y"), "-peers=host:"}, // invalid address
	} {
		if err := execute(t.Context(), args, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
