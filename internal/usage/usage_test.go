package usage

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestCounterRollsOverDaily(t *testing.T) {
	var buf bytes.Buffer
	now := time.Date(2026, 10, 15, 23, 59, 0, 0, time.UTC)
	c := &Counter{Log: slog.New(slog.NewTextHandler(&buf, nil)), Now: func() time.Time { return now }}

	c.Add("webhook_event", 40)
	c.Add("webhook_op", 2)
	c.Add("das", 3)
	day, counts, credits := c.Snapshot()
	if day != "2026-10-15" || counts["webhook_event"] != 40 || credits != 40+200+30 {
		t.Fatalf("snapshot = %s %v %d", day, counts, credits)
	}

	now = now.Add(2 * time.Minute) // next UTC day
	c.Add("webhook_event", 1)
	if !strings.Contains(buf.String(), "usage: day closed") || !strings.Contains(buf.String(), "est_credits=270") {
		t.Errorf("rollover log missing:\n%s", buf.String())
	}
	day, counts, _ = c.Snapshot()
	if day != "2026-10-16" || counts["webhook_event"] != 1 {
		t.Errorf("after rollover: %s %v", day, counts)
	}
}

func TestNilCounter(t *testing.T) {
	var c *Counter
	c.Add("x", 1) // must not panic
}
