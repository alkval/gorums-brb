package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/alkval/gorums-brb/internal/cluster"
	"github.com/alkval/gorums-brb/internal/peertls"
)

// generateCerts creates a project CA and one credential directory per replica.
// Run it on the provisioning machine, keep ca-key.pem there, and copy only
// node-<id> to replica <id>.
func generateCerts(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("brb -mode=certs", flag.ContinueOnError)
	flags.SetOutput(output)
	out := flags.String("out", "", "new output directory, outside the repository")
	peers := flags.String("peers", "", "comma-separated IP:port addresses, ordered by node ID as in -remotes")
	validity := flags.Duration("validity", 30*24*time.Hour, "certificate lifetime")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *out == "" || strings.TrimSpace(*peers) == "" || *validity <= 0 {
		return errors.New("brb certs: requires -out, -peers and a positive -validity")
	}
	addrs, err := cluster.Peers(strings.Split(*peers, ","), false)
	if err != nil {
		return err
	}
	if err := peertls.Generate(*out, addrs, *validity); err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "brb certs: wrote %s for %d replicas; keep %s on this machine and copy only node-<id> to replica <id>\n",
		*out, len(addrs), peertls.CAKeyFile)
	return err
}
