package brb

import (
	"crypto/sha256"
	"errors"
	"fmt"
)

// DigestSize is the length of a READY digest in the READY-digest variant.
const DigestSize = sha256.Size

// NewReadyDigestStateMachine implements Bracha with the full value in PROPOSE
// and ECHO and only its SHA-256 digest in READY. Safety additionally assumes
// that SHA-256 is collision resistant. A READY quorum without a matching value
// stays pending until a PROPOSE or ECHO supplies one. Under reliable links that
// value always arrives, because an ECHO quorum behind the READYs contains at
// least f+1 correct replicas that sent ECHO with the full value to everyone.
func NewReadyDigestStateMachine(n, f int) (*StateMachine, error) {
	m, err := NewStateMachine(n, f)
	if err != nil {
		return nil, err
	}
	m.readyDigest = true
	return m, nil
}

// HandleReadyDigest records a READY vote for digest in the READY-digest variant.
func (m *StateMachine) HandleReadyDigest(from ProcessID, id BroadcastID, digest []byte) ([]Action, error) {
	if !m.readyDigest {
		return nil, errors.New("brb: READY digests are not used in this variant")
	}
	if len(digest) != DigestSize {
		return nil, fmt.Errorf("brb: READY digest has %d bytes, want %d", len(digest), DigestSize)
	}
	return m.handle(from, PhaseReady, id, nil, digest)
}

// handleDigest applies one admitted message in the READY-digest variant. The
// caller holds m.mu. Repeated and conflicting messages from a sender are
// dropped before hashing, so they cost no hashing and add no stored values.
func (m *StateMachine) handleDigest(state *instance, from ProcessID, phase Phase, id BroadcastID, value, digest []byte) []Action {
	var actions []Action
	switch phase {
	case PhasePropose:
		if state.proposed {
			return nil
		}
		state.proposed = true
		digest = digestOf(value)
		state.storePayload(digest, value)
		if !state.echoSent {
			state.echoSent = true
			actions = append(actions, newAction(ActionMulticastEcho, id, value))
		}
	case PhaseEcho:
		if state.echoes.seen[from] {
			return nil
		}
		digest = digestOf(value)
		state.storePayload(digest, value)
		count := state.echoes.add(from, digest)
		if !state.readySent && uint64(count) > (uint64(m.n)+uint64(m.f))/2 {
			state.readySent = true
			actions = append(actions, readyDigestAction(id, digest))
		}
	case PhaseReady:
		// A replica may amplify READY without holding the value.
		if count := state.readies.add(from, digest); !state.readySent && count > m.f {
			state.readySent = true
			actions = append(actions, readyDigestAction(id, digest))
		}
	}
	return append(actions, m.tryDeliver(state, id, digest)...)
}

// tryDeliver delivers once digest has more than 2f READYs and a value with that
// digest is stored. Each accepted message changes the votes or the stored value
// of one digest, so checking that digest after the message finds every newly
// enabled delivery. The value is copied before the instance state is cleared.
func (m *StateMachine) tryDeliver(state *instance, id BroadcastID, digest []byte) []Action {
	if state.delivered || state.readies.counts[string(digest)] <= m.f*2 {
		return nil
	}
	value, ok := state.payloads[string(digest)]
	if !ok {
		return nil
	}
	action := newAction(ActionDeliver, id, value)
	// Retain out-of-order deliveries until every earlier ID delivered.
	*state = instance{delivered: true}
	m.compact(id.Origin)
	return []Action{action}
}

// storePayload keeps the first value seen for digest. Presence in the map, not
// a non-empty value, marks it available, so an empty value is handled too.
func (s *instance) storePayload(digest, value []byte) {
	if s.payloads == nil {
		s.payloads = make(map[string][]byte)
	}
	if _, ok := s.payloads[string(digest)]; !ok {
		s.payloads[string(digest)] = append([]byte(nil), value...)
	}
}

func digestOf(value []byte) []byte {
	sum := sha256.Sum256(value)
	return sum[:]
}

func readyDigestAction(id BroadcastID, digest []byte) Action {
	return Action{Type: ActionMulticastReady, ID: id, Digest: append([]byte(nil), digest...)}
}
