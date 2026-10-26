package peertls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"net"
	"net/url"
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
	if info, err := os.Stat(out); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("output directory: %v %v", info, err)
	}
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
}

func TestGenerateRefusesExistingOutput(t *testing.T) {
	// A permissive directory that already exists must not receive a CA key.
	open := filepath.Join(t.TempDir(), "open")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := Generate(open, testAddrs, time.Hour); err == nil {
		t.Fatal("Generate wrote into an existing permissive directory")
	}
	if entries, _ := os.ReadDir(open); len(entries) != 0 {
		t.Fatalf("Generate left files in the refused directory: %v", entries)
	}
	// Existing credentials stay exactly as they were.
	out := generate(t)
	snapshot := func() map[string]string {
		files := map[string]string{}
		_ = filepath.WalkDir(out, func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				data, _ := os.ReadFile(path)
				files[path] = string(data)
			}
			return nil
		})
		return files
	}
	before := snapshot()
	if err := Generate(out, testAddrs, time.Hour); err == nil {
		t.Fatal("Generate overwrote existing credentials")
	}
	after := snapshot()
	if len(after) != len(before) {
		t.Fatalf("credential files changed: %d before, %d after", len(before), len(after))
	}
	for path, data := range before {
		if after[path] != data {
			t.Fatalf("%s changed", path)
		}
	}
}

// issue writes a credential directory for id whose certificate has the given
// extended key usages and validity, signed by the CA generated in out.
func issue(t *testing.T, out string, id uint32, usages []x509.ExtKeyUsage, notBefore, notAfter time.Time) string {
	t.Helper()
	ca, err := tls.LoadX509KeyPair(filepath.Join(out, CAFile), filepath.Join(out, CAKeyFile))
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse(Identity(id))
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "test node"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
		URIs:         []*url.URL{uri},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}, ca.Leaf, &key.PublicKey, ca.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "issued")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(filepath.Join(out, CAFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, CAFile), caPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writePEM(filepath.Join(dir, CertFile), "CERTIFICATE", der, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeKey(filepath.Join(dir, KeyFile), key); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoadRequiresBothTLSRoles(t *testing.T) {
	out := generate(t)
	now := time.Now()
	server := x509.ExtKeyUsageServerAuth
	client := x509.ExtKeyUsageClientAuth
	if _, _, err := Load(issue(t, out, 1, []x509.ExtKeyUsage{server, client}, now.Add(-time.Minute), now.Add(time.Hour)), 1); err != nil {
		t.Fatalf("dual-purpose certificate: %v", err)
	}
	for name, tc := range map[string]struct {
		usages              []x509.ExtKeyUsage
		notBefore, notAfter time.Time
		want                string
	}{
		"server authentication only": {[]x509.ExtKeyUsage{server}, now.Add(-time.Minute), now.Add(time.Hour), "not valid for client authentication"},
		"client authentication only": {[]x509.ExtKeyUsage{client}, now.Add(-time.Minute), now.Add(time.Hour), "not valid for server authentication"},
		"expired":                    {[]x509.ExtKeyUsage{server, client}, now.Add(-2 * time.Hour), now.Add(-time.Hour), "expired"},
		"not yet valid":              {[]x509.ExtKeyUsage{server, client}, now.Add(time.Hour), now.Add(2 * time.Hour), "not yet valid"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := issue(t, out, 1, tc.usages, tc.notBefore, tc.notAfter)
			if _, _, err := Load(dir, 1); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load = %v, want error containing %q", err, tc.want)
			}
		})
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
		}), 1, "unknown authority"},
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
		"malformed certificate": {variant(t, func(dir string) {
			_ = os.WriteFile(filepath.Join(dir, CertFile), []byte("not a certificate"), 0o644)
		}), 1, "certificate"},
		"malformed CA": {variant(t, func(dir string) {
			_ = os.WriteFile(filepath.Join(dir, CAFile), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("junk")}), 0o644)
		}), 1, "no CA certificate"},
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
