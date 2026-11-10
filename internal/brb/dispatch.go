package brb

import (
	"context"
	"sync"
	"time"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"github.com/relab/gorums"
)

// slotCharge is the logical per-entry overhead added to each queued message
// or delivery, so that empty values still consume byte credit.
const slotCharge = 32

// credits is the logical occupancy of one bounded queue. Active work is the
// queued items plus the one item a worker has taken but not yet finished, so
// a stalled send or callback keeps its credit until it returns.
type credits struct {
	queuedEntries, inFlightEntries int
	queuedBytes, inFlightBytes     int64
	maxEntries                     int
	maxBytes                       int64
	highEntries                    int
	highBytes                      int64
}

func (c *credits) fits(charge int64) bool {
	return c.queuedEntries+c.inFlightEntries+1 <= c.maxEntries &&
		c.queuedBytes+c.inFlightBytes+charge <= c.maxBytes
}

func (c *credits) add(charge int64) {
	c.queuedEntries++
	c.queuedBytes += charge
	c.highEntries = max(c.highEntries, c.queuedEntries+c.inFlightEntries)
	c.highBytes = max(c.highBytes, c.queuedBytes+c.inFlightBytes)
}

func (c *credits) take(charge int64) {
	c.queuedEntries--
	c.queuedBytes -= charge
	c.inFlightEntries++
	c.inFlightBytes += charge
}

func (c *credits) release(charge int64) {
	c.inFlightEntries--
	c.inFlightBytes -= charge
}

// fifo is a slice-backed queue that reuses its storage once drained.
type fifo[T any] struct {
	items []T
	head  int
}

func (q *fifo[T]) len() int { return len(q.items) - q.head }

func (q *fifo[T]) push(item T) { q.items = append(q.items, item) }

func (q *fifo[T]) pop() T {
	var zero T
	item := q.items[q.head]
	q.items[q.head] = zero
	q.head++
	switch {
	case q.head == len(q.items):
		q.items, q.head = q.items[:0], 0
	case q.head >= 1024 && 2*q.head >= len(q.items):
		q.items, q.head = append(q.items[:0], q.items[q.head:]...), 0
	}
	return item
}

// drop empties the queue and returns the number of discarded items.
func (q *fifo[T]) drop() int {
	count := q.len()
	clear(q.items)
	q.items, q.head = nil, 0
	return count
}

// wake signals a waiting worker without blocking the producer.
func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

type peerItem struct {
	phase  Phase
	id     BroadcastID
	msg    *brbv1.PhaseMessage // shared by every recipient; never modified
	call   multicastFunc
	charge int64
}

// peerQueue holds the messages for one remote peer, sent in order by that
// peer's worker through a configuration containing only that peer. Producers
// hold Replica.dispatchMu; the worker never does.
type peerQueue struct {
	id     ProcessID
	config gorums.Config
	wakeup chan struct{}

	mu               sync.Mutex
	items            fifo[peerItem]
	credits          credits
	closed           bool
	discardedEntries int
	discardedBytes   int64
}

func newPeerQueue(peer *gorums.Node, maxEntries int, maxBytes int64) *peerQueue {
	return &peerQueue{
		id:      ProcessID(peer.ID()),
		config:  gorums.Config{peer},
		wakeup:  make(chan struct{}, 1),
		credits: credits{maxEntries: maxEntries, maxBytes: maxBytes},
	}
}

// fits reports whether one more item of this charge is within capacity.
func (q *peerQueue) fits(charge int64) (bool, credits) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.credits.fits(charge), q.credits
}

func (q *peerQueue) push(item peerItem) {
	q.mu.Lock()
	q.items.push(item)
	q.credits.add(item.charge)
	q.mu.Unlock()
	wake(q.wakeup)
}

// next waits for the next item and moves its credit to in flight. It returns
// false once the queue is closed or ctx ends.
func (q *peerQueue) next(ctx context.Context) (peerItem, bool) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return peerItem{}, false
		}
		if q.items.len() > 0 {
			item := q.items.pop()
			q.credits.take(item.charge)
			q.mu.Unlock()
			return item, true
		}
		q.mu.Unlock()
		select {
		case <-q.wakeup:
		case <-ctx.Done():
			return peerItem{}, false
		}
	}
}

func (q *peerQueue) done(item peerItem) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.credits.release(item.charge)
}

// close discards unsent items and records them. The connections are already
// closed by then, so they could not be sent.
func (q *peerQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	q.discardedEntries = q.credits.queuedEntries
	q.discardedBytes = q.credits.queuedBytes
	q.credits.queuedEntries, q.credits.queuedBytes = 0, 0
	q.items.drop()
	wake(q.wakeup)
}

func (q *peerQueue) snapshot() PeerQueueSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	return PeerQueueSnapshot{Peer: q.id, Queue: queueSnapshot(q.credits),
		DiscardedAtStopEntries: q.discardedEntries, DiscardedAtStopBytes: q.discardedBytes}
}

type selfItem struct {
	delivery Delivery
	charge   int64
}

// selfQueue holds local deliveries for the single callback worker. Producers
// hold Replica.dispatchMu; the worker never does. Closing it stops intake but
// lets the worker finish every queued delivery.
type selfQueue struct {
	wakeup chan struct{}

	mu      sync.Mutex
	items   fifo[selfItem]
	credits credits
	closed  bool
	maxLag  time.Duration
}

func newSelfQueue(maxEntries int, maxBytes int64) *selfQueue {
	return &selfQueue{wakeup: make(chan struct{}, 1), credits: credits{maxEntries: maxEntries, maxBytes: maxBytes}}
}

// push queues d, or reports false with the occupancy if it does not fit.
func (q *selfQueue) push(d Delivery, charge int64) (bool, credits) {
	q.mu.Lock()
	if q.closed || !q.credits.fits(charge) {
		defer q.mu.Unlock()
		return false, q.credits
	}
	q.items.push(selfItem{d, charge})
	q.credits.add(charge)
	q.mu.Unlock()
	wake(q.wakeup)
	return true, credits{}
}

// next waits for the next delivery. It returns false once intake is closed
// and every queued delivery has been taken.
func (q *selfQueue) next() (selfItem, bool) {
	for {
		q.mu.Lock()
		if q.items.len() > 0 {
			item := q.items.pop()
			q.credits.take(item.charge)
			q.mu.Unlock()
			return item, true
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return selfItem{}, false
		}
		<-q.wakeup
	}
}

func (q *selfQueue) started(lag time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxLag = max(q.maxLag, lag)
}

func (q *selfQueue) done(item selfItem) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.credits.release(item.charge)
}

func (q *selfQueue) closeIntake() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	wake(q.wakeup)
}

func (q *selfQueue) snapshot() SelfQueueSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	return SelfQueueSnapshot{Queue: queueSnapshot(q.credits), MaxCallbackLag: q.maxLag}
}
