package cluster

import "testing"

func TestFaultBounds(t *testing.T) {
	for _, tc := range []struct{ n, input, want int }{{1, -1, 0}, {3, -1, 0}, {4, -1, 1}, {7, -1, 2}, {10, -1, 3}, {15, -1, 4}, {7, 1, 1}, {4, 0, 0}} {
		f, err := FaultBound(tc.n, tc.input)
		if err != nil || f != tc.want {
			t.Fatalf("n=%d input=%d: %d, %v", tc.n, tc.input, f, err)
		}
	}
	for _, tc := range [][2]int{{0, -1}, {6, 2}, {4, -2}} {
		if _, err := FaultBound(tc[0], tc[1]); err == nil {
			t.Fatalf("accepted %v", tc)
		}
	}
}

func TestPeerNormalization(t *testing.T) {
	for _, tc := range [][]string{{"127.0.0.1:1", "[::ffff:127.0.0.1]:1"}, {"0.0.0.0:1"}, {"127.0.0.1:0"}, {"host:1"}} {
		if _, err := Peers(tc, false); err == nil {
			t.Fatalf("accepted %v", tc)
		}
	}
	if p, err := Peers([]string{"[::1]:9000", "127.0.0.1:9001"}, false); err != nil || len(p) != 2 {
		t.Fatalf("%v %v", p, err)
	}
}
