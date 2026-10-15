// Package budget enforces a hard daily cap on Helius webhook events.
//
// Every delivered event costs a credit, and the free plan has 1M a month.
// Per-address limits alone don't bound the total (20 addresses x 10
// events/min is still ~290k events a day), so this is the backstop: when
// the day's count reaches Limit, solwatch pauses (alerts stop and the
// webhook is deleted, so Helius stops billing) until the next UTC day.
//
// The count is persisted so a restart doesn't hand out a fresh budget.
package budget

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// KV is where the counter is persisted.
type KV interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
}

const (
	keyDay   = "budget_day"
	keyCount = "budget_events"
)

type Budget struct {
	Limit int64 // events per UTC day; <= 0 disables the budget
	KV    KV
	Now   func() time.Time

	// OnPause runs once when the limit is reached; OnResume when a new UTC
	// day starts after a pause. Both run synchronously, in the caller's
	// goroutine.
	OnPause  func(ctx context.Context)
	OnResume func(ctx context.Context)

	mu     sync.Mutex
	day    string
	count  int64
	paused bool
}

func (b *Budget) now() time.Time {
	if b.Now != nil {
		return b.Now()
	}
	return time.Now()
}

func today(t time.Time) string { return t.UTC().Format("2006-01-02") }

// Load restores today's count from KV. Call once at startup, before
// anything else touches the budget. If today's budget is already spent,
// the budget starts paused (without calling OnPause again).
func (b *Budget) Load(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.day = today(b.now())
	day, err := b.KV.Get(ctx, keyDay)
	if err != nil {
		return err
	}
	if day == b.day {
		v, err := b.KV.Get(ctx, keyCount)
		if err != nil {
			return err
		}
		b.count, _ = strconv.ParseInt(v, 10, 64)
	}
	b.paused = b.Limit > 0 && b.count >= b.Limit
	return nil
}

// Paused reports whether today's budget is spent.
func (b *Budget) Paused() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.paused
}

// Count returns the day and events counted so far.
func (b *Budget) Count() (string, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.day, b.count
}

// Add counts n delivered events. Events that arrive while paused (in the
// seconds before the webhook delete lands) are still counted.
func (b *Budget) Add(ctx context.Context, n int) {
	if b == nil {
		return
	}
	b.rollover(ctx)
	b.mu.Lock()
	b.count += int64(n)
	count, day := b.count, b.day
	trip := b.Limit > 0 && !b.paused && count >= b.Limit
	if trip {
		b.paused = true
	}
	b.mu.Unlock()

	_ = b.KV.Set(ctx, keyDay, day)
	_ = b.KV.Set(ctx, keyCount, strconv.FormatInt(count, 10))
	if trip && b.OnPause != nil {
		b.OnPause(ctx)
	}
}

// Tick checks for the UTC day rollover. Call it every minute or so; Add
// also checks, but a paused demo receives no events to trigger it.
func (b *Budget) Tick(ctx context.Context) { b.rollover(ctx) }

func (b *Budget) rollover(ctx context.Context) {
	b.mu.Lock()
	d := today(b.now())
	if d == b.day {
		b.mu.Unlock()
		return
	}
	wasPaused := b.paused
	b.day, b.count, b.paused = d, 0, false
	b.mu.Unlock()

	_ = b.KV.Set(ctx, keyDay, d)
	_ = b.KV.Set(ctx, keyCount, "0")
	if wasPaused && b.OnResume != nil {
		b.OnResume(ctx)
	}
}

// Run calls Tick every interval until ctx ends.
func (b *Budget) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.Tick(ctx)
		}
	}
}
