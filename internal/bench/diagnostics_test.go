package bench

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/relab/gorums"
)

func TestOriginTimingSummary(t *testing.T) {
	var o originTiming
	base := time.Now()
	for range 5 {
		o.start()
	}
	o.finish(base.Add(150*time.Millisecond), base, false) // returned 150 ms after delivery
	o.finish(base, base.Add(time.Millisecond), false)     // returned before delivery
	o.finish(base, time.Time{}, true)                     // failed, never delivered
	o.finish(base.Add(5*time.Millisecond), base, true)    // send failure, delivered anyway
	got := o.summary()                                    // one call still outstanding
	want := originSummary{Started: 5, Returned: 4, Failed: 2, Outstanding: 1, BeforeDelivery: 1, AfterDelivery: 2,
		Undelivered: 1, P50: 150 * time.Millisecond, P99: 150 * time.Millisecond, Max: 150 * time.Millisecond,
		Over: [3]uint64{1, 1, 0}}
	if got != want {
		t.Fatalf("summary %+v, want %+v", got, want)
	}
}

var (
	originLine = regexp.MustCompile(`brb bench: node (\d+) diag origin started=(\d+) returned=(\d+) failed=(\d+) ` +
		`outstanding=(\d+) before_delivery=(\d+) after_delivery=(\d+) undelivered=(\d+) after_p50_ns=(\d+) ` +
		`after_p99_ns=(\d+) after_max_ns=(\d+) after_over10ms=(\d+) after_over100ms=(\d+) after_over1s=(\d+)\n`)
	sendLine = regexp.MustCompile(`brb bench: node (\d+) diag send phase=(\w+) started=(\d+) succeeded=(\d+) ` +
		`failed=(\d+) cancelled=(\d+) outstanding=(\d+) total_ns=(\d+) max_ns=(\d+) over10ms=(\d+) ` +
		`over100ms=(\d+) over1s=(\d+) oldest_outstanding_ns=(\d+)\n`)
)

func numbers(match []string) []uint64 {
	out := make([]uint64, len(match)-1)
	for i, s := range match[1:] {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			out[i] = v
		}
	}
	return out
}

// checkDiagnostics checks that replica id printed exactly one consistent
// origin line, one send line per phase and one window line. It returns the
// origin line's values: started, returned, failed, outstanding,
// before_delivery, after_delivery, undelivered, then the delay statistics.
func checkDiagnostics(t *testing.T, logged string, id int) []uint64 {
	t.Helper()
	var origins [][]uint64
	for _, m := range originLine.FindAllStringSubmatch(logged, -1) {
		if v := numbers(m); v[0] == uint64(id) {
			origins = append(origins, v[1:])
		}
	}
	if len(origins) != 1 {
		t.Fatalf("node %d: %d diag origin lines in:\n%s", id, len(origins), logged)
	}
	o := origins[0] // started returned failed outstanding before after undelivered p50 p99 max over10 over100 over1s
	if o[0] == 0 || o[0] != o[1]+o[3] || o[1] != o[4]+o[5]+o[6] || o[2] > o[1] ||
		o[7] > o[8] || o[8] > o[9] || o[10] > o[5] || o[11] > o[10] || o[12] > o[11] {
		t.Fatalf("node %d: inconsistent diag origin %v", id, o)
	}
	phases := map[string][]uint64{}
	for _, m := range sendLine.FindAllStringSubmatch(logged, -1) {
		if m[1] == strconv.Itoa(id) {
			if _, dup := phases[m[2]]; dup {
				t.Fatalf("node %d: duplicate diag send line for %s", id, m[2])
			}
			phases[m[2]] = numbers(append([]string{m[0]}, m[3:]...))
		}
	}
	for _, phase := range []string{"propose", "echo", "ready", "qecho"} {
		s, ok := phases[phase] // started succeeded failed cancelled outstanding total max over10 over100 over1s oldest
		if !ok {
			t.Fatalf("node %d: missing diag send line for %s in:\n%s", id, phase, logged)
		}
		completed := s[1] + s[2] + s[3]
		if s[0] != completed+s[4] || (completed > 0 && s[6] > s[5]) || s[8] > s[7] || s[9] > s[8] || s[7] > completed ||
			(s[4] == 0 && s[10] != 0) {
			t.Fatalf("node %d: inconsistent diag send %s %v", id, phase, s)
		}
		if (phase == "propose" || phase == "echo" || phase == "ready") && s[0] == 0 {
			t.Fatalf("node %d: no %s sends recorded", id, phase)
		}
	}
	if n := strings.Count(logged, fmt.Sprintf("brb bench: node %d diag window violations=", id)); n != 1 {
		t.Fatalf("node %d: %d diag window lines", id, n)
	}
	return o
}

func TestAwaitDeliveryKeepsBufferedDelivery(t *testing.T) {
	delivered := time.Now()
	buffered := func() chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- delivered
		return ch
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	stopped := func() chan error {
		ch := make(chan error, 1)
		ch <- errors.New("listener closed")
		return ch
	}

	if got, err := awaitDelivery(t.Context(), buffered(), nil); err != nil || !got.Equal(delivered) {
		t.Fatalf("delivery: %v %v", got, err)
	}
	if got, err := awaitDelivery(cancelled, make(chan time.Time, 1), nil); !errors.Is(err, context.Canceled) || !got.IsZero() {
		t.Fatalf("unobserved delivery: %v %v, want zero time and context.Canceled", got, err)
	}
	// With both ready, select picks either case; the delivery is kept in both.
	for range 200 {
		got, err := awaitDelivery(cancelled, buffered(), nil)
		if !got.Equal(delivered) || (err != nil && !errors.Is(err, context.Canceled)) {
			t.Fatalf("cancellation and delivery ready: %v %v", got, err)
		}
		got, err = awaitDelivery(t.Context(), buffered(), stopped())
		if !got.Equal(delivered) || (err != nil && !errors.Is(err, errServerStopped)) {
			t.Fatalf("server stop and delivery ready: %v %v", got, err)
		}
	}
	got, err := awaitDelivery(t.Context(), make(chan time.Time, 1), stopped())
	if !got.IsZero() || !errors.Is(err, errServerStopped) || err.Error() != "brb bench: server stopped: listener closed" {
		t.Fatalf("server stopped: %v %v", got, err)
	}
}

// settleBroadcast keeps a delivery buffered before an attempt ends, whatever
// ends it, and returns the error unchanged.
func TestSettleBroadcastKeepsBufferedDelivery(t *testing.T) {
	delivered := time.Now()
	returned := delivered.Add(50 * time.Millisecond)
	buffered := func() chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- delivered
		return ch
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	broken := errors.New("brb: sequence already used")
	for _, c := range []struct {
		name           string
		ctx            context.Context
		ch             chan time.Time
		err, wantErr   error
		after, undeliv uint64
	}{
		{"broadcast failure after delivery", t.Context(), buffered(), broken, broken, 1, 0},
		{"broadcast failure, nothing delivered", t.Context(), make(chan time.Time, 1), broken, broken, 0, 1},
		{"send failure, then delivery", t.Context(), buffered(), gorums.ErrSendFailure, nil, 1, 0},
		{"cancelled, nothing delivered", cancelled, make(chan time.Time, 1), nil, context.Canceled, 0, 1},
		{"cancelled after send failure and delivery", cancelled, buffered(), gorums.ErrSendFailure, nil, 1, 0},
	} {
		var o originTiming
		o.start()
		_, err := settleBroadcast(c.ctx, &o, c.ch, nil, returned, c.err)
		got := o.summary()
		// With cancellation and a delivery both ready, either error outcome is valid.
		errOK := errors.Is(err, c.wantErr) || (c.wantErr == nil && c.ctx == cancelled && errors.Is(err, context.Canceled))
		if !errOK || got.AfterDelivery != c.after || got.Undelivered != c.undeliv || got.Outstanding != 0 {
			t.Errorf("%s: err %v, summary %+v", c.name, err, got)
		}
		if c.after == 1 && got.Max != 50*time.Millisecond {
			t.Errorf("%s: delay %v, want 50ms", c.name, got.Max)
		}
	}
}
