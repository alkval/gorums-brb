package peertls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// CAKeyFile holds the CA signing key in the output root. Keep it on the
// provisioning machine and never copy it to a replica.
const CAKeyFile = "ca-key.pem"

// Generate creates a project CA and one credential directory per replica,
// node-<id>, for the ordered addresses. Each directory holds only that
// replica's key and certificate plus the public CA certificate, ready to copy
// to that host. Certificates are valid for the given duration.
func Generate(out string, addrs []string, validity time.Duration) error {
	if len(addrs) == 0 {
		return fmt.Errorf("peertls: no addresses")
	}
	if err := os.MkdirAll(out, 0o700); err != nil {
		return err
	}
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "gorums-brb project CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(out, CAFile), "CERTIFICATE", caDER, 0o644); err != nil {
		return err
	}
	if err := writeKey(filepath.Join(out, CAKeyFile), caKey); err != nil {
		return err
	}
	for i, addr := range addrs {
		id := uint32(i + 1)
		uri, ips, dns, err := sans(id, addr)
		if err != nil {
			return err
		}
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		template := &x509.Certificate{
			SerialNumber: serial(),
			Subject:      pkix.Name{CommonName: "gorums-brb node " + strconv.FormatUint(uint64(id), 10)},
			NotBefore:    now.Add(-time.Minute),
			NotAfter:     now.Add(validity),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			URIs:         []*url.URL{uri},
			IPAddresses:  ips,
			DNSNames:     dns,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			return err
		}
		dir := filepath.Join(out, "node-"+strconv.FormatUint(uint64(id), 10))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		if err := writePEM(filepath.Join(dir, CAFile), "CERTIFICATE", caDER, 0o644); err != nil {
			return err
		}
		if err := writePEM(filepath.Join(dir, CertFile), "CERTIFICATE", der, 0o644); err != nil {
			return err
		}
		if err := writeKey(filepath.Join(dir, KeyFile), key); err != nil {
			return err
		}
	}
	return nil
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return n
}

func writeKey(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	return writePEM(path, "PRIVATE KEY", der, 0o600)
}

// writePEM creates path exclusively, so existing credentials are never
// overwritten.
func writePEM(path, kind string, der []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if err := pem.Encode(f, &pem.Block{Type: kind, Bytes: der}); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
