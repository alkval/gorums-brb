package bench

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

const (
	netFileBytes = 2 << 20
	netFileRows  = 16384
	netSockets   = 256
	netOutputMax = 256 << 10
)

// Endpoint addresses and inodes are used only for filtering and lifecycle
// tracking. Neither is included in any diagnostic record or error text.
type procSocket struct {
	local, remote netip.AddrPort
	state         uint8
	tx, rx        uint64
	timer         uint8
	ticks, rto    uint64
	probes, inode uint64
}

func procEndpoint(s string, little bool) (netip.AddrPort, error) {
	host, port, ok := strings.Cut(s, ":")
	if !ok {
		return netip.AddrPort{}, errors.New("endpoint format")
	}
	b, err := hex.DecodeString(host)
	if err != nil || (len(b) != 4 && len(b) != 16) {
		return netip.AddrPort{}, errors.New("endpoint address")
	}
	if little {
		for i := 0; i < len(b); i += 4 {
			b[i], b[i+3] = b[i+3], b[i]
			b[i+1], b[i+2] = b[i+2], b[i+1]
		}
	}
	p, err := strconv.ParseUint(port, 16, 16)
	if err != nil {
		return netip.AddrPort{}, errors.New("endpoint port")
	}
	a, ok := netip.AddrFromSlice(b)
	if !ok {
		return netip.AddrPort{}, errors.New("endpoint address")
	}
	return netip.AddrPortFrom(a.Unmap(), uint16(p)), nil
}

func nativeLittleEndian() bool {
	return binary.NativeEndian.Uint16([]byte{1, 0}) == 1
}

func hexPair(s string) (uint64, uint64, error) {
	a, b, ok := strings.Cut(s, ":")
	if !ok {
		return 0, 0, errors.New("pair format")
	}
	x, err := strconv.ParseUint(a, 16, 64)
	if err != nil {
		return 0, 0, errors.New("pair number")
	}
	y, err := strconv.ParseUint(b, 16, 64)
	if err != nil {
		return 0, 0, errors.New("pair number")
	}
	return x, y, nil
}

func parseSockets(data []byte, little bool) ([]procSocket, int, error) {
	if len(data) > netFileBytes {
		return nil, 0, errors.New("byte_limit")
	}
	scan := bufio.NewScanner(bytes.NewReader(data))
	scan.Buffer(make([]byte, 4096), netFileBytes)
	if !scan.Scan() || !strings.Contains(scan.Text(), "local_address") {
		return nil, 0, errors.New("parse_error")
	}
	var sockets []procSocket
	rows := 0
	for scan.Scan() {
		if strings.TrimSpace(scan.Text()) == "" {
			continue
		}
		rows++
		if rows > netFileRows {
			return nil, rows, errors.New("entry_limit")
		}
		f := strings.Fields(scan.Text())
		if len(f) < 4 {
			return nil, rows, errors.New("parse_error")
		}
		st, err := strconv.ParseUint(f[3], 16, 8)
		if err != nil {
			return nil, rows, errors.New("parse_error")
		}
		if st == 10 { // LISTEN queues have different meanings; no peer connection.
			continue
		}
		if len(f) < 10 {
			return nil, rows, errors.New("parse_error")
		}
		var s procSocket
		s.state = uint8(st)
		if s.local, err = procEndpoint(f[1], little); err != nil {
			return nil, rows, errors.New("parse_error")
		}
		if s.remote, err = procEndpoint(f[2], little); err != nil {
			return nil, rows, errors.New("parse_error")
		}
		if s.state == 1 { // Only ESTABLISHED fields are interpreted as byte queues.
			if len(f) < 10 {
				return nil, rows, errors.New("parse_error")
			}
			if s.tx, s.rx, err = hexPair(f[4]); err != nil {
				return nil, rows, errors.New("parse_error")
			}
			var timer uint64
			if timer, s.ticks, err = hexPair(f[5]); err != nil || timer > 255 {
				return nil, rows, errors.New("parse_error")
			}
			s.timer = uint8(timer)
			if s.rto, err = strconv.ParseUint(f[6], 16, 64); err != nil {
				return nil, rows, errors.New("parse_error")
			}
			if s.probes, err = strconv.ParseUint(f[8], 10, 64); err != nil {
				return nil, rows, errors.New("parse_error")
			}
			if s.inode, err = strconv.ParseUint(f[9], 10, 64); err != nil {
				return nil, rows, errors.New("parse_error")
			}
		}
		sockets = append(sockets, s)
	}
	if scan.Err() != nil {
		return nil, rows, errors.New("parse_error")
	}
	return sockets, rows, nil
}

// parseCounters reads header/value pairs, ignoring unrelated counters (some
// of which, such as MaxConn, can legitimately be negative).
func parseCounters(data []byte, prefix string, names []string) (map[string]uint64, error) {
	if len(data) > netFileBytes {
		return nil, errors.New("byte_limit")
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) > netFileRows {
		return nil, errors.New("entry_limit")
	}
	var result map[string]uint64
	for i := 0; i < len(lines); i++ {
		h := strings.Fields(lines[i])
		if len(h) == 0 || h[0] != prefix+":" {
			continue
		}
		if result != nil || i+1 >= len(lines) {
			return nil, errors.New("parse_error")
		}
		i++
		v := strings.Fields(lines[i])
		if len(v) != len(h) || v[0] != h[0] {
			return nil, errors.New("parse_error")
		}
		result = make(map[string]uint64)
		seen := make(map[string]bool)
		for j, name := range h[1:] {
			if seen[name] {
				return nil, errors.New("parse_error")
			}
			seen[name] = true
			for _, want := range names {
				if name == want {
					n, err := strconv.ParseUint(v[j+1], 10, 64)
					if err != nil {
						return nil, errors.New("parse_error")
					}
					result[name] = n
				}
			}
		}
	}
	if len(result) != len(names) {
		return nil, errors.New("parse_error")
	}
	return result, nil
}

type socketKey struct {
	Dialer     uint32 `json:"dialer"`
	Acceptor   uint32 `json:"acceptor"`
	ClientPort uint16 `json:"client_port"`
}

type socketRecord struct {
	socketKey
	Peer        uint32  `json:"peer"`
	Role        string  `json:"role"`
	State       uint8   `json:"state"`
	TX          *uint64 `json:"tx_queue"`
	RX          *uint64 `json:"rx_queue"`
	Timer       *uint8  `json:"timer_active"`
	Ticks       *uint64 `json:"timer_ticks"`
	RTO         *uint64 `json:"unrecovered_rto"`
	Probes      *uint64 `json:"zero_window_probes"`
	Generation  uint64  `json:"generation"`
	Lifecycle   string  `json:"lifecycle"`
	Reconnect   bool    `json:"reconnect"`
	Ambiguous   bool    `json:"ambiguous"`
	FieldStatus string  `json:"field_status"`
}

type netMembership struct {
	selfID uint32
	self   netip.AddrPort
	byAddr map[netip.AddrPort]uint32
	byIP   map[netip.Addr][]uint32
}

func newNetMembership(self uint32, peers []string) (netMembership, error) {
	m := netMembership{selfID: self, byAddr: make(map[netip.AddrPort]uint32), byIP: make(map[netip.Addr][]uint32)}
	if len(peers) > netSockets/2+1 {
		return m, errors.New("brb bench: diag-net membership exceeds collector capacity")
	}
	for i, raw := range peers {
		a, err := netip.ParseAddrPort(raw)
		if err != nil || a.Addr().Zone() != "" {
			return m, errors.New("brb bench: diag-net requires numeric, unzoned peer endpoints")
		}
		a = netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
		if _, exists := m.byAddr[a]; exists {
			return m, errors.New("brb bench: duplicate diag-net peer endpoint")
		}
		id := uint32(i + 1)
		m.byAddr[a] = id
		m.byIP[a.Addr()] = append(m.byIP[a.Addr()], id)
		if self == id {
			m.self = a
		}
	}
	if !m.self.IsValid() {
		return m, errors.New("brb bench: invalid diag-net identity")
	}
	return m, nil
}

func (m netMembership) match(s procSocket) (socketRecord, bool) {
	var r socketRecord
	if s.local.Addr() != m.self.Addr() || s.remote.Port() == 0 {
		return r, false
	}
	if peer, ok := m.byAddr[s.remote]; ok && peer != m.selfID && s.local.Port() != m.self.Port() {
		r = socketRecord{socketKey: socketKey{m.selfID, peer, s.local.Port()}, Peer: peer, Role: "dialed", Ambiguous: len(m.byIP[s.local.Addr()]) > 1}
	} else if s.local == m.self {
		var candidates []uint32
		for _, id := range m.byIP[s.remote.Addr()] {
			if id != m.selfID {
				candidates = append(candidates, id)
			}
		}
		if len(candidates) == 0 {
			return r, false
		}
		var peer uint32
		if len(candidates) == 1 {
			peer = candidates[0]
		}
		r = socketRecord{socketKey: socketKey{peer, m.selfID, s.remote.Port()}, Peer: peer, Role: "accepted", Ambiguous: peer == 0}
	} else {
		return r, false
	}
	r.State, r.FieldStatus = s.state, "state_only"
	if s.state == 1 {
		r.TX, r.RX, r.Timer, r.Ticks, r.RTO, r.Probes = &s.tx, &s.rx, &s.timer, &s.ticks, &s.rto, &s.probes
		r.FieldStatus = "ok"
	}
	return r, true
}

type counterRecord struct {
	Name        string  `json:"name"`
	Status      string  `json:"status"`
	Value       *uint64 `json:"value"`
	DeltaStatus string  `json:"delta_status"`
	Delta       *uint64 `json:"delta"`
}

func counterDelta(name, status string, current, previous *uint64) counterRecord {
	r := counterRecord{Name: name, Status: status, Value: current, DeltaStatus: "unavailable"}
	if current == nil {
		return r
	}
	if previous == nil {
		r.DeltaStatus = "initial"
	} else if *current < *previous {
		r.DeltaStatus = "reset"
	} else {
		d := *current - *previous
		r.Delta, r.DeltaStatus = &d, "ok"
	}
	return r
}

func parseStatus(err error) string {
	if err == nil {
		return "ok"
	}
	switch err.Error() {
	case "byte_limit", "entry_limit":
		return err.Error()
	default:
		return "parse_error"
	}
}

func netStatusError(status string) error {
	return fmt.Errorf("brb bench: diag-net %s", status)
}
