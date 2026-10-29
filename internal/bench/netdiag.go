package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const netJoinTimeout = 2 * time.Second

type netSource struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Bytes   int    `json:"bytes"`
	Entries int    `json:"entries"`
}

type netRound struct {
	Status     string          `json:"status"`
	Sources    []netSource     `json:"sources"`
	Sockets    int             `json:"sockets"`
	Counters   []counterRecord `json:"counters"`
	Problems   []string        `json:"problems"`
	Unobserved []netPeer       `json:"unobserved"`
}

type netPeer struct {
	Peer uint32 `json:"peer"`
	Role string `json:"role"`
}

type netMissed struct {
	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
	Count uint64 `json:"count"`
}

type netTerminal struct {
	Status string `json:"status"`
}

// Every line has the read interval and UTC anchor. Reads of different files
// and the two ends of a connection are not simultaneous snapshots.
type netRecord struct {
	Version  int           `json:"version"`
	Round    uint64        `json:"round"`
	Kind     string        `json:"kind"`
	Phase    string        `json:"phase"`
	StartMS  int64         `json:"start_ms"`
	EndMS    int64         `json:"end_ms"`
	Anchor   string        `json:"anchor_utc"`
	Socket   *socketRecord `json:"socket,omitempty"`
	Summary  *netRound     `json:"summary,omitempty"`
	Missed   *netMissed    `json:"missed,omitempty"`
	Terminal *netTerminal  `json:"terminal,omitempty"`
}

type socketLife struct {
	inode, generation uint64
	present           bool
}

// netCollector has one worker for reading, parsing and writing. Its clock is
// independent of the resource sampler. read and write must not launch workers.
// A blocked read or write is abandoned at shutdown. No replacement worker is
// started. An in-flight write may finish later; only a best-effort terminal
// record may follow it. Diagnostic failures never invalidate a benchmark.
type netCollector struct {
	self                uint32
	membership          netMembership
	interval            time.Duration
	read                func(context.Context, string) ([]byte, error)
	write               func([]byte) error
	close               func()
	now                 func() time.Time
	joinTimeout         time.Duration
	stopRequest, done   chan struct{}
	cancel              context.CancelFunc
	ctx                 context.Context
	beginOnce, stopOnce sync.Once
	started             bool
	closed              atomic.Bool
	stopErr             error
	shutdownStatus      string // owned by stopOnce, readable after stop returns
	sinkUnavailable     bool   // fixed by the constructor before begin
	err                 error  // worker-owned until done is closed
	failure             string // worker-owned terminal status
	currentRound        uint64
	currentPhase        string
	history             map[socketKey]socketLife
	previous            map[string]*uint64
}

func makeNetCollector(self uint32, peers []string, interval time.Duration,
	read func(context.Context, string) ([]byte, error), write func([]byte) error, closeFn func()) (*netCollector, error) {
	if interval <= 0 {
		return nil, netStatusError("requires a positive interval")
	}
	m, err := newNetMembership(self, peers)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &netCollector{self: self, membership: m, interval: interval, read: read, write: write,
		close: closeFn, now: time.Now, joinTimeout: netJoinTimeout, stopRequest: make(chan struct{}), done: make(chan struct{}),
		ctx: ctx, cancel: cancel, history: make(map[socketKey]socketLife), previous: make(map[string]*uint64)}, nil
}

func (c *netCollector) begin(start time.Time) {
	c.beginOnce.Do(func() {
		c.started = true
		go c.run(start)
	})
}

func (c *netCollector) stop() error {
	c.stopOnce.Do(func() {
		c.shutdownStatus = "ok"
		if c.sinkUnavailable {
			c.shutdownStatus = "unavailable_sink"
		}
		if !c.started {
			c.shutdownStatus = "not_started"
			c.closed.Store(true)
			c.cancel()
			c.close()
			return
		}
		close(c.stopRequest)
		timer := time.NewTimer(c.joinTimeout)
		defer timer.Stop()
		select {
		case <-c.done:
			c.stopErr = c.err
			if c.err != nil && !c.sinkUnavailable {
				c.shutdownStatus = c.failure
			}
		case <-timer.C:
			c.closed.Store(true)
			c.cancel()
			c.stopErr = netStatusError("shutdown_timeout (incomplete network diagnostics)")
			c.shutdownStatus = "shutdown_timeout"
		}
	})
	return c.stopErr
}

func (c *netCollector) run(start time.Time) {
	defer close(c.done)
	defer c.close()
	defer c.cancel()
	defer c.logTerminal(start)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	var round, lastSlot uint64
	final := func() {
		slot := uint64(c.now().Sub(start) / c.interval)
		c.collect(start, round+1, "final", missedNetSlots(lastSlot, slot))
	}
	if !c.collect(start, round, "initial", nil) {
		return
	}
	for {
		// Give a pending final round priority over a stale ticker event.
		select {
		case <-c.stopRequest:
			final()
			return
		default:
		}
		select {
		case <-c.ctx.Done():
			return
		case <-c.stopRequest:
			final()
			return
		case <-ticker.C:
			slot := uint64(c.now().Sub(start) / c.interval)
			if slot <= lastSlot {
				continue
			}
			missed := missedNetSlots(lastSlot, slot)
			lastSlot, round = slot, round+1
			if !c.collect(start, round, "periodic", missed) {
				return
			}
		}
	}
}

func (c *netCollector) collect(start time.Time, round uint64, phase string, missed *netMissed) bool {
	c.currentRound, c.currentPhase = round, phase
	begin := c.now()
	result := netRound{Status: "ok", Sources: []netSource{}, Counters: []counterRecord{}, Problems: []string{}, Unobserved: []netPeer{}}
	var matched []struct {
		record socketRecord
		inode  uint64
	}
	complete := true
	for _, name := range []string{"tcp", "tcp6", "snmp", "netstat"} {
		if c.ctx.Err() != nil {
			return false
		}
		data, err := c.read(c.ctx, name)
		src := netSource{Name: name, Status: "ok", Bytes: len(data)}
		var counters map[string]uint64
		if err != nil {
			src.Status = "read_error"
		} else if name == "tcp" || name == "tcp6" {
			var sockets []procSocket
			sockets, src.Entries, err = parseSockets(data, nativeLittleEndian())
			src.Status = parseStatus(err)
			if err == nil {
				for _, s := range sockets {
					if r, ok := c.membership.match(s); ok {
						if len(matched) == netSockets {
							src.Status = "socket_limit"
							break
						}
						matched = append(matched, struct {
							record socketRecord
							inode  uint64
						}{r, s.inode})
					}
				}
			}
		} else {
			prefix, names := "Tcp", []string{"RetransSegs", "OutSegs"}
			if name == "netstat" {
				prefix, names = "TcpExt", []string{"TCPTimeouts"}
			}
			counters, err = parseCounters(data, prefix, names)
			src.Status = parseStatus(err)
			if err == nil {
				src.Entries = len(counters)
			}
		}
		if src.Status != "ok" {
			result.Status = "partial"
			if name == "tcp" || name == "tcp6" {
				complete = false
			}
		}
		result.Sources = append(result.Sources, src)
		if name == "snmp" || name == "netstat" {
			names := []string{"RetransSegs", "OutSegs"}
			if name == "netstat" {
				names = []string{"TCPTimeouts"}
			}
			for _, key := range names {
				var value *uint64
				if src.Status == "ok" {
					v := counters[key]
					value = &v
				}
				result.Counters = append(result.Counters, counterDelta(key, src.Status, value, c.previous[key]))
				c.previous[key] = value // a missing round breaks the delta baseline
			}
		}
	}
	counts := make(map[socketKey]int)
	for _, s := range matched {
		counts[s.record.socketKey]++
	}
	seen := make(map[socketKey]bool)
	sockets := make([]socketRecord, 0, len(matched))
	for _, s := range matched {
		r := s.record
		r.Lifecycle = "unknown"
		r.Ambiguous = r.Ambiguous || counts[r.socketKey] > 1
		life, exists := c.history[r.socketKey]
		if !r.Ambiguous && s.inode != 0 {
			if !exists && len(c.history) >= netSockets {
				result.Status = "partial"
				result.Problems = appendUnique(result.Problems, "history_limit")
			} else {
				r.Lifecycle = "observed"
				if !exists {
					life.generation = 1
					r.Lifecycle = "first"
					for key := range c.history {
						if key.Dialer == r.Dialer && key.Acceptor == r.Acceptor && key.ClientPort != r.ClientPort {
							r.Reconnect, r.Ambiguous = true, true // another socket, not proof of reconnection
						}
					}
				} else if !life.present || life.inode != s.inode {
					life.generation++
					r.Lifecycle, r.Reconnect = "reappeared_or_replaced", true
				}
				life.inode, life.present = s.inode, true
				c.history[r.socketKey] = life
				r.Generation = life.generation
			}
		}
		if r.Ambiguous {
			result.Status = "partial"
			result.Problems = appendUnique(result.Problems, "ambiguous_socket")
		}
		seen[r.socketKey] = true
		sockets = append(sockets, r)
	}
	if complete {
		for key, life := range c.history {
			if !seen[key] {
				life.present = false
				c.history[key] = life
			}
		}
	}
	sort.Slice(sockets, func(i, j int) bool {
		a, b := sockets[i], sockets[j]
		if a.Dialer != b.Dialer {
			return a.Dialer < b.Dialer
		}
		if a.Acceptor != b.Acceptor {
			return a.Acceptor < b.Acceptor
		}
		return a.ClientPort < b.ClientPort
	})
	result.Sockets = len(sockets)
	observed := make(map[netPeer]bool)
	for _, s := range sockets {
		if s.Peer != 0 {
			observed[netPeer{s.Peer, s.Role}] = true
		}
	}
	for peer := uint32(1); int(peer) <= len(c.membership.byAddr); peer++ {
		if peer == c.self {
			continue
		}
		for _, role := range []string{"dialed", "accepted"} {
			p := netPeer{peer, role}
			if !observed[p] {
				result.Unobserved = append(result.Unobserved, p)
			}
		}
	}
	end := c.now()
	base := netRecord{Version: 1, Round: round, Phase: phase, StartMS: begin.Sub(start).Milliseconds(), EndMS: end.Sub(start).Milliseconds(), Anchor: start.UTC().Format(time.RFC3339Nano)}
	var lines [][]byte
	add := func(r netRecord) {
		b, _ := json.Marshal(r) // only concrete structs and integer/string fields
		lines = append(lines, fmt.Appendf(nil, "brb bench: node %d diag net %s\n", c.self, b))
	}
	if missed != nil {
		r := base
		r.Kind, r.Missed = "missed", missed
		add(r)
	}
	for _, s := range sockets {
		r := base
		r.Kind, r.Socket = "socket", &s
		add(r)
	}
	base.Kind, base.Summary = "round", &result
	add(base)
	total := 0
	for _, line := range lines {
		total += len(line)
		if len(line) > 4096 || total > netOutputMax {
			c.failure = "output_limit"
			c.err = netStatusError(c.failure)
			return false
		}
	}
	for _, line := range lines {
		if c.closed.Load() || c.ctx.Err() != nil {
			return false
		}
		if err := c.write(line); err != nil {
			c.failure = "output_unavailable"
			c.err = netStatusError(c.failure)
			return false
		}
	}
	return true
}

// The current slot is represented by this round, including the final round.
func missedNetSlots(last, current uint64) *netMissed {
	if current <= last+1 {
		return nil
	}
	return &netMissed{last + 1, current - 1, current - last - 1}
}

// Only the existing worker attempts this line. It may itself block or fail,
// so stop never waits beyond its join budget for the marker to arrive.
func (c *netCollector) logTerminal(start time.Time) {
	status := c.failure
	if c.closed.Load() {
		status = "shutdown_timeout"
	}
	if status == "" {
		return
	}
	at := c.now().Sub(start).Milliseconds()
	r := netRecord{Version: 1, Round: c.currentRound, Kind: "terminal", Phase: c.currentPhase,
		StartMS: at, EndMS: at, Anchor: start.UTC().Format(time.RFC3339Nano), Terminal: &netTerminal{Status: status}}
	b, _ := json.Marshal(r)
	_ = c.write(fmt.Appendf(nil, "brb bench: node %d diag net %s\n", c.self, b))
}

func appendUnique(values []string, value string) []string {
	if slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}
