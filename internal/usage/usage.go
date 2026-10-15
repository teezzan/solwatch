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
	Logger *slog.Logger
	Now    func() time.Time // for tests

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

// Add records n units of kind (e.g. "webhook_event").
func (c *Counter) Add(kind string, n int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rolloverLocked()
	c.counts[kind] += int64(n)
}

// rolloverLocked starts a new UTC day if needed, first logging the daily
// summary for the day that just ended.
func (c *Counter) rolloverLocked() {
	day := c.now().UTC().Format("2006-01-02")
	if c.day == day {
		return
	}
	if c.day != "" {
		c.logLocked("daily usage summary")
	}
	c.day, c.counts = day, map[string]int64{}
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

// Run logs a usage line every interval, and the daily summary as soon as
// a UTC day ends (checked every minute), until ctx ends.
func (c *Counter) Run(ctx context.Context, every time.Duration) {
	logT := time.NewTicker(every)
	defer logT.Stop()
	dayT := time.NewTicker(time.Minute)
	defer dayT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-dayT.C:
			c.mu.Lock()
			c.rolloverLocked()
			c.mu.Unlock()
		case <-logT.C:
			c.Log("usage")
		}
	}
}

// Log writes today's counts with the given message, e.g. at shutdown.
func (c *Counter) Log(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logLocked(msg)
}

func (c *Counter) logLocked(msg string) {
	if c.Logger == nil {
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
	c.Logger.Info(msg, attrs...)
}
