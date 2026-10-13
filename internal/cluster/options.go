// Package cluster validates the fixed membership used by both command modes.
package cluster

import (
	"fmt"
	"net"
	"net/netip"
	"strings"

	"github.com/alkval/gorums-brb/internal/brb"
)

// Peers normalizes addresses and rejects duplicate or wildcard endpoints.
// Only the terminal demo resolves names. Sweeps supply numeric addresses.
func Peers(peers []string, resolveNames bool) ([]string, error) {
	out := make([]string, len(peers))
	seen := make(map[string]bool)
	for i, value := range peers {
		value = strings.TrimSpace(value)
		addr, err := netip.ParseAddrPort(value)
		if err != nil && resolveNames {
			resolved, resolveErr := net.ResolveTCPAddr("tcp", value)
			if resolveErr != nil {
				return nil, fmt.Errorf("brb: resolve %q: %w", value, resolveErr)
			}
			addr = resolved.AddrPort()
			err = nil
		}
		if err != nil || !addr.IsValid() || addr.Port() == 0 || addr.Addr().IsUnspecified() {
			return nil, fmt.Errorf("brb: %q must identify a non-wildcard IP and positive port", value)
		}
		addr = netip.AddrPortFrom(addr.Addr().Unmap(), addr.Port())
		out[i] = addr.String()
		if seen[out[i]] {
			return nil, fmt.Errorf("brb: duplicate peer %q", value)
		}
		seen[out[i]] = true
	}
	return out, nil
}

// FaultBound defaults to the greatest f satisfying n > 3f.
func FaultBound(n, f int) (int, error) {
	if f == -1 {
		f = (n - 1) / 3
	}
	_, err := brb.NewStateMachine(n, f)
	return f, err
}
