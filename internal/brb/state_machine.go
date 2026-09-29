package brb

import (
	"errors"
	"fmt"
	"sync"
)

// ProcessID identifies one process in the fixed membership.
type ProcessID uint32

// BroadcastID identifies one broadcast from a designated origin.
type BroadcastID struct {
	Origin   ProcessID
	Sequence uint64
}

// Phase identifies a protocol message type.
type Phase uint8

const (
	PhaseSend Phase = iota + 1
	PhaseEcho
	PhaseReady
)

// ActionType identifies work that the transport adapter must perform.
type ActionType uint8

const (
	ActionMulticastEcho ActionType = iota + 1
	ActionMulticastReady
	ActionDeliver
)

// Action is emitted by the state machine after processing one message.
type Action struct {
	Type  ActionType
	ID    BroadcastID
	Value []byte
}

// Machine owns the state for every broadcast instance observed by one process.
// It is safe for concurrent use. Completed IDs remain until the machine is
// discarded, so delayed messages cannot cause a second delivery.
type Machine struct {
	mu        sync.Mutex
	n         int
	f         int
	instances map[BroadcastID]*instance
}

type instance struct {
	echoSent  bool
	readySent bool
	delivered bool
	echoes    votes
	readies   votes
}

// Keep only the first vote from each sender in each phase. A correct sender
// never changes its vote. Conflicting later votes from faulty senders add no
// useful evidence and must not allocate more storage.
type votes struct {
	seen   map[ProcessID]bool
	counts map[string]int
}

func (v *votes) add(from ProcessID, value []byte) int {
	if v.seen[from] {
		return 0
	}
	if v.seen == nil {
		v.seen = make(map[ProcessID]bool)
		v.counts = make(map[string]int)
	}
	v.seen[from] = true
	key := string(value) // the map owns an immutable copy
	v.counts[key]++
	return v.counts[key]
}

// NewMachine constructs a state machine for a fixed membership whose process
// IDs are 1 through n. The n > 3f constraint is the provisional textbook model.
func NewMachine(n, f int) (*Machine, error) {
	if n < 1 {
		return nil, errors.New("brb: n must be positive")
	}
	if f < 0 {
		return nil, errors.New("brb: f must not be negative")
	}
	if f > (n-1)/3 {
		return nil, fmt.Errorf("brb: requires n > 3f, got n=%d f=%d", n, f)
	}
	if uint64(n) > uint64(^uint32(0)) {
		return nil, errors.New("brb: membership exceeds process ID range")
	}
	return &Machine{
		n:         n,
		f:         f,
		instances: make(map[BroadcastID]*instance),
	}, nil
}

// Handle records one protocol message and returns the actions newly enabled by
// it. Messages are counted by distinct logical sender. The current prototype
// expects the transport adapter to provide that sender identity.
func (m *Machine) Handle(from ProcessID, phase Phase, id BroadcastID, value []byte) ([]Action, error) {
	if from == 0 || uint64(from) > uint64(m.n) {
		return nil, fmt.Errorf("brb: sender ID %d is outside membership 1..%d", from, m.n)
	}
	if id.Origin == 0 || uint64(id.Origin) > uint64(m.n) {
		return nil, fmt.Errorf("brb: origin ID %d is outside membership 1..%d", id.Origin, m.n)
	}
	if id.Sequence == 0 {
		return nil, errors.New("brb: broadcast sequence must be positive")
	}
	if phase < PhaseSend || phase > PhaseReady {
		return nil, fmt.Errorf("brb: unknown phase %d", phase)
	}
	if phase == PhaseSend && from != id.Origin {
		return nil, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.instances[id]
	if state == nil {
		state = &instance{}
		m.instances[id] = state
	}
	if state.delivered {
		return nil, nil
	}

	var actions []Action
	switch phase {
	case PhaseSend:
		if !state.echoSent {
			state.echoSent = true
			actions = append(actions, newAction(ActionMulticastEcho, id, value))
		}
	case PhaseEcho:
		count := state.echoes.add(from, value)
		if !state.readySent && uint64(count) > (uint64(m.n)+uint64(m.f))/2 {
			state.readySent = true
			actions = append(actions, newAction(ActionMulticastReady, id, value))
		}
	case PhaseReady:
		readyCount := state.readies.add(from, value)
		if !state.readySent && readyCount > m.f {
			state.readySent = true
			actions = append(actions, newAction(ActionMulticastReady, id, value))
		}
		if readyCount > 2*m.f {
			// Retain a tombstone so delayed messages cannot restart this instance.
			*state = instance{delivered: true}
			actions = append(actions, newAction(ActionDeliver, id, value))
		}
	}
	return actions, nil
}

func newAction(actionType ActionType, id BroadcastID, value []byte) Action {
	return Action{Type: actionType, ID: id, Value: append([]byte(nil), value...)}
}
