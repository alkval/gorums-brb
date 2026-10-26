// Package peertls provides the TLS credentials and node identities that let
// Gorums verify which replica sent each message.
package peertls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/relab/gorums"
	"google.golang.org/grpc/credentials"
)

// Files in a replica's credential directory. The directory holds only this
// replica's key; the CA signing key never belongs there.
const (
	CAFile   = "ca.pem"
	CertFile = "node.pem"
	KeyFile  = "node-key.pem"
)

const identityPrefix = "spiffe://gorums-brb/node/"

// Identity is the certificate URI that binds a replica ID to its certificate.
func Identity(id uint32) string { return identityPrefix + strconv.FormatUint(uint64(id), 10) }

// Node is a peer address with its expected TLS identity. Passing such nodes to
// gorums.WithPeers makes Gorums verify every peer stream before any handler
// runs, in both directions.
type Node struct{ address, identity string }

func (n Node) Addr() string        { return n.address }
func (n Node) TLSIdentity() string { return n.identity }

// NodeFor returns replica id at addr with its identity, for example inside
// gorums.WithLocalNodes once local listeners are allocated.
func NodeFor(id uint32, addr string) Node { return Node{addr, Identity(id)} }

// Nodes assigns IDs 1..n to the ordered addresses, as the address-only
// gorums.WithNodeList does, and attaches each replica's identity.
func Nodes(addrs []string) map[gorums.ID]Node {
	nodes := make(map[gorums.ID]Node, len(addrs))
	for i, addr := range addrs {
		id := uint32(i + 1)
		nodes[id] = NodeFor(id, addr)
	}
	return nodes
}

// Load reads the credential directory of replica self. It fails unless the
// certificate chains to the CA in the same directory, names self's identity,
// matches the private key, and the key is readable only by its owner. There is
// no fallback: a caller that requested TLS must not continue without it.
func Load(dir string, self uint32) (server, client credentials.TransportCredentials, err error) {
	caPEM, err := os.ReadFile(filepath.Join(dir, CAFile))
	if err != nil {
		return nil, nil, fmt.Errorf("peertls: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("peertls: no CA certificate in %s", CAFile)
	}
	keyPath := filepath.Join(dir, KeyFile)
	info, err := os.Stat(keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("peertls: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, nil, fmt.Errorf("peertls: %s must not be readable by group or others (mode %v)", KeyFile, info.Mode().Perm())
	}
	pair, err := tls.LoadX509KeyPair(filepath.Join(dir, CertFile), keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("peertls: %w", err)
	}
	leaf := pair.Leaf
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return nil, nil, fmt.Errorf("peertls: %s does not chain to %s: %w", CertFile, CAFile, err)
	}
	if !hasURI(leaf, Identity(self)) {
		return nil, nil, fmt.Errorf("peertls: %s does not name %s", CertFile, Identity(self))
	}
	server = credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{pair},
		ClientCAs:    pool,
		// Replicas are the only clients, so anonymous connections are refused.
		ClientAuth: tls.RequireAndVerifyClientCert,
	})
	client = credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
	})
	return server, client, nil
}

func hasURI(cert *x509.Certificate, identity string) bool {
	for _, u := range cert.URIs {
		if u.String() == identity {
			return true
		}
	}
	return false
}

// sans returns the identity URI and the host of addr for a replica certificate.
// gRPC also checks the dialed host, so the certificate must name it.
func sans(id uint32, addr string) (*url.URL, []net.IP, []string, error) {
	uri, err := url.Parse(Identity(id))
	if err != nil {
		return nil, nil, nil, err
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("peertls: address %q: %w", addr, err)
	}
	if ip := net.ParseIP(host); ip != nil {
		return uri, []net.IP{ip}, nil, nil
	}
	if host == "" {
		return nil, nil, nil, errors.New("peertls: address has no host")
	}
	return uri, nil, []string{host}, nil
}
