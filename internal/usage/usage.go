// Package usage keeps a per-day tally of billable Helius activity and logs
// it, so the credit burn of a public demo is visible in `kubectl logs`.
package usage

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Credits per unit, from https://www.helius.dev/docs/billing/credits and
// the webhook docs. Reads of the webhook API aren't listed as billable.
var Credits = map[string]int64{
	"webhook_event": 1,
	"webhook_op":    100,
	"das":           10,
	"rpc":           1,
}

type Counter struct {
	Log *slog.Logger
	Now func() time.Time // for tests

	mu     sync.Mutex
	day    string
	counts map[string]int64
}

func (c *Counter) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// Add records n units of kind (e.g. "webhook_event"). On the first Add of a
// new UTC day, yesterday's totals are logged and reset.
func (c *Counter) Add(kind string, n int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	day := c.now().UTC().Format("2006-01-02")
	if c.day != day {
		if c.day != "" {
			c.logLocked("usage: day closed")
		}
		c.day, c.counts = day, map[string]int64{}
	}
	c.counts[kind] += int64(n)
}

// Snapshot returns today's counts and the estimated credits they cost.
func (c *Counter) Snapshot() (day string, counts map[string]int64, credits int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts = make(map[string]int64, len(c.counts))
	for k, v := range c.counts {
		counts[k] = v
		credits += v * Credits[k]
	}
	return c.day, counts, credits
}

// Run logs a usage line every interval until ctx ends, and once more on exit.
func (c *Counter) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			c.mu.Lock()
			c.logLocked("usage: shutdown")
			c.mu.Unlock()
			return
		case <-t.C:
			c.mu.Lock()
			c.logLocked("usage")
			c.mu.Unlock()
		}
	}
}

func (c *Counter) logLocked(msg string) {
	if c.Log == nil {
		return
	}
	keys := make([]string, 0, len(c.counts))
	for k := range c.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	attrs := []any{"day", c.day}
	var credits int64
	for _, k := range keys {
		attrs = append(attrs, k, c.counts[k])
		credits += c.counts[k] * Credits[k]
	}
	attrs = append(attrs, "est_credits", credits)
	c.Log.Info(msg, attrs...)
}
