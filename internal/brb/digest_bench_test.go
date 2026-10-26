package brb

import (
	"bytes"
	"fmt"
	"testing"
)

// BenchmarkTransition measures one PROPOSE and one ECHO transition of the state
// machine, without networking, for Bracha and the READY-digest variant at
// n=13, f=4. The gap is the incremental cost of those two transitions in these
// implementations, including the digest variant's extra allocations, not the
// cost of SHA-256 alone. It does not give the cost of a whole broadcast, in
// which a replica handles one PROPOSE, up to n ECHOs and up to n READYs. Run it
// with
//
//	go test -run '^$' -bench Transition ./internal/brb
func BenchmarkTransition(b *testing.B) {
	for _, size := range []int{256, 4096, 16384} {
		for _, variant := range []struct {
			name       string
			newMachine func(n, f int) (*StateMachine, error)
		}{{"bracha", NewStateMachine}, {"ready-digest", NewReadyDigestStateMachine}} {
			b.Run(fmt.Sprintf("%s/%dB", variant.name, size), func(b *testing.B) {
				value := bytes.Repeat([]byte("x"), size)
				fresh := func() *StateMachine {
					m, err := variant.newMachine(13, 4)
					if err != nil {
						b.Fatal(err)
					}
					return m
				}
				m := fresh()
				seq := uint64(0)
				b.SetBytes(int64(2 * size))
				b.ReportAllocs()
				for b.Loop() {
					// Undelivered instances fill the receive window, so start a
					// new machine outside the timed region when it is full.
					if seq++; seq > WindowSize {
						b.StopTimer()
						m, seq = fresh(), 1
						b.StartTimer()
					}
					id := BroadcastID{1, seq}
					if _, err := m.Handle(1, PhasePropose, id, value); err != nil {
						b.Fatal(err)
					}
					if _, err := m.Handle(2, PhaseEcho, id, value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
