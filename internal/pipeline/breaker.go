package pipeline

import (
	"sync"
	"time"
)

// Breaker trips when one address produces more than Max events inside a
// one-minute window. Every webhook event costs a credit whether or not we
// alert on it, so a single market maker or bot wallet could burn a free
// plan's monthly credits in a day. When it trips, the caller unwatches the
// address for everyone.
type Breaker struct {
	Max    int           // events per Window; <= 0 disables the breaker
	Window time.Duration // default 1 minute

	mu      sync.Mutex
	windows map[string]*window
}

type window struct {
	start   time.Time
	count   int
	tripped bool
}

// Observe records one event for addr at now. It returns true exactly once
// per window: on the event that pushes the count over Max.
func (b *Breaker) Observe(addr string, now time.Time) (tripped bool, count int) {
	if b == nil || b.Max <= 0 {
		return false, 0
	}
	span := b.Window
	if span <= 0 {
		span = time.Minute
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.windows == nil {
		b.windows = map[string]*window{}
	}
	w := b.windows[addr]
	if w == nil || now.Sub(w.start) >= span {
		w = &window{start: now}
		b.windows[addr] = w
		b.gc(now, span)
	}
	w.count++
	if w.count > b.Max && !w.tripped {
		w.tripped = true
		return true, w.count
	}
	return false, w.count
}

// gc drops stale windows so the map doesn't grow without bound.
func (b *Breaker) gc(now time.Time, span time.Duration) {
	for a, w := range b.windows {
		if now.Sub(w.start) >= 2*span {
			delete(b.windows, a)
		}
	}
}
