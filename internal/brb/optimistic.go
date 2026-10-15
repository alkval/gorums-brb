package brb

import "fmt"

// NewOptimisticStateMachine implements the optimistic path of OptiBRB, with
// deterministic committees instead of reputation. There is no recovery: a
// selected faulty member can prevent delivery, including totality.
func NewOptimisticStateMachine(n, f int, self ProcessID) (*StateMachine, error) {
	m, err := NewStateMachine(n, f)
	if err != nil {
		return nil, err
	}
	if self == 0 || uint64(self) > uint64(n) {
		return nil, fmt.Errorf("brb: optimistic identity %d outside membership", self)
	}
	m.optimistic, m.self = true, self
	return m, nil
}

// The three committees are overlapping prefixes of a cyclic membership order.
// Advancing either origin or sequence rotates every role. Computing the offset
// modulo n before adding avoids overflow for large sequence numbers.
func (m *StateMachine) committeeContains(id BroadcastID, member ProcessID, size int) bool {
	start := (uint64(id.Origin-1) + (id.Sequence-1)%uint64(m.n)) % uint64(m.n)
	position := (uint64(member-1) + uint64(m.n) - start) % uint64(m.n)
	return position < uint64(size)
}

func (m *StateMachine) accepts(from ProcessID, phase Phase, id BroadcastID) bool {
	q := (m.n+m.f)/2 + 1
	switch phase {
	case PhasePropose:
		return m.committeeContains(id, m.self, q)
	case PhaseEcho:
		return m.committeeContains(id, from, q) && m.committeeContains(id, m.self, m.f+1)
	case PhaseQEcho:
		return m.committeeContains(id, from, m.f+1) && m.committeeContains(id, m.self, 2*m.f+1)
	case PhaseReady:
		return m.committeeContains(id, from, 2*m.f+1)
	}
	return false
}
