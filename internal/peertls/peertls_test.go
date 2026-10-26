package peertls

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

var testAddrs = []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}

func generate(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "creds")
	if err := Generate(out, testAddrs, time.Hour); err != nil {
		t.Fatal(err)
	}
	return out
}

func nodeDir(out string, id int) string {
	return filepath.Join(out, "node-"+strconv.Itoa(id))
}

func TestGenerateSeparatesCredentialsPerReplica(t *testing.T) {
	out := generate(t)
	if info, err := os.Stat(filepath.Join(out, CAKeyFile)); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("CA key: %v %v", info, err)
	}
	for id := 1; id <= len(testAddrs); id++ {
		dir := nodeDir(out, id)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		// Only this replica's key and certificate plus the public CA certificate.
		if want := []string{CAFile, KeyFile, CertFile}; !slices.Equal(names, want) {
			t.Fatalf("node %d files = %v, want %v", id, names, want)
		}
		if info, _ := os.Stat(dir); info.Mode().Perm() != 0o700 {
			t.Fatalf("node %d directory mode %v", id, info.Mode().Perm())
		}
		if info, _ := os.Stat(filepath.Join(dir, KeyFile)); info.Mode().Perm() != 0o600 {
			t.Fatalf("node %d key mode %v", id, info.Mode().Perm())
		}
		if _, _, err := Load(dir, uint32(id)); err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
	}
	if err := Generate(out, testAddrs, time.Hour); err == nil {
		t.Fatal("Generate overwrote existing credentials")
	}
}

func TestLoadFailsClosed(t *testing.T) {
	out := generate(t)
	other := generate(t) // a different CA
	copyFile := func(t *testing.T, src, dst string) {
		t.Helper()
		data, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		info, _ := os.Stat(src)
		if err := os.WriteFile(dst, data, info.Mode().Perm()); err != nil {
			t.Fatal(err)
		}
	}
	// variant copies node 1's directory and applies one change to it.
	variant := func(t *testing.T, change func(dir string)) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "node")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{CAFile, CertFile, KeyFile} {
			copyFile(t, filepath.Join(nodeDir(out, 1), name), filepath.Join(dir, name))
		}
		change(dir)
		return dir
	}
	for name, tc := range map[string]struct {
		dir  string
		self uint32
		want string
	}{
		"missing directory": {filepath.Join(out, "absent"), 1, "no such file"},
		"wrong identity":    {nodeDir(out, 2), 1, "does not name"},
		"untrusted CA": {variant(t, func(dir string) {
			copyFile(t, filepath.Join(other, CAFile), filepath.Join(dir, CAFile))
		}), 1, "does not chain"},
		"mismatched key": {variant(t, func(dir string) {
			_ = os.Remove(filepath.Join(dir, KeyFile))
			copyFile(t, filepath.Join(nodeDir(out, 2), KeyFile), filepath.Join(dir, KeyFile))
		}), 1, "private key does not match"},
		"key readable by others": {variant(t, func(dir string) {
			_ = os.Chmod(filepath.Join(dir, KeyFile), 0o644)
		}), 1, "must not be readable"},
		"missing certificate": {variant(t, func(dir string) {
			_ = os.Remove(filepath.Join(dir, CertFile))
		}), 1, "no such file"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := Load(tc.dir, tc.self); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestNodesAssignIdentitiesInOrder(t *testing.T) {
	nodes := Nodes(testAddrs)
	for i, addr := range testAddrs {
		n := nodes[uint32(i+1)]
		if n.Addr() != addr || n.TLSIdentity() != Identity(uint32(i+1)) {
			t.Fatalf("node %d = %+v", i+1, n)
		}
	}
	if Identity(3) != "spiffe://gorums-brb/node/3" {
		t.Fatalf("identity format changed: %s", Identity(3))
	}
}
