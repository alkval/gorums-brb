//go:build !linux

package bench

import "time"

const netDiagSupported = false

func newNetCollector(uint32, []string, time.Duration) (*netCollector, error) {
	return nil, netStatusError("requires Linux")
}
