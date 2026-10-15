package budget

import (
	"context"
	"testing"
	"time"
)

type memKV map[string]string

func (m memKV) Get(_ context.Context, k string) (string, error) { return m[k], nil }
func (m memKV) Set(_ context.Context, k, v string) error        { m[k] = v; return nil }

func TestBudgetPausesAndResumesAtUTCMidnight(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 15, 22, 0, 0, 0, time.UTC)
	kv := memKV{}
	var pauses, resumes int
	b := &Budget{Limit: 10, KV: kv, Now: func() time.Time { return now },
		OnPause: func(context.Context) { pauses++ }, OnResume: func(context.Context) { resumes++ }}
	if err := b.Load(ctx); err != nil {
		t.Fatal(err)
	}

	b.Add(ctx, 6)
	if b.Paused() || pauses != 0 {
		t.Fatal("paused early")
	}
	b.Add(ctx, 4) // hits 10
	if !b.Paused() || pauses != 1 {
		t.Fatalf("paused=%v pauses=%d after reaching the limit", b.Paused(), pauses)
	}
	b.Add(ctx, 3) // stragglers before the webhook delete lands
	if pauses != 1 {
		t.Errorf("OnPause called %d times, want 1", pauses)
	}
	if _, n := b.Count(); n != 13 {
		t.Errorf("count = %d, want 13 (stragglers still cost credits)", n)
	}

	now = now.Add(time.Hour) // 23:00 UTC, same day
	b.Tick(ctx)
	if !b.Paused() || resumes != 0 {
		t.Fatal("resumed before midnight")
	}
	now = now.Add(time.Hour + time.Second) // 00:00:01 UTC
	b.Tick(ctx)
	if b.Paused() || resumes != 1 {
		t.Fatalf("paused=%v resumes=%d after midnight", b.Paused(), resumes)
	}
	if day, n := b.Count(); day != "2026-10-16" || n != 0 {
		t.Errorf("after rollover: %s %d", day, n)
	}
}

func TestBudgetSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	kv := memKV{}
	b1 := &Budget{Limit: 10, KV: kv, Now: func() time.Time { return now }}
	b1.Load(ctx)
	b1.Add(ctx, 10)

	// New process, same day: still paused, and OnPause isn't re-sent.
	pauses := 0
	b2 := &Budget{Limit: 10, KV: kv, Now: func() time.Time { return now }, OnPause: func(context.Context) { pauses++ }}
	b2.Load(ctx)
	if !b2.Paused() || pauses != 0 {
		t.Fatalf("after restart: paused=%v pauses=%d", b2.Paused(), pauses)
	}

	// New process, next day: fresh budget.
	now = now.Add(24 * time.Hour)
	b3 := &Budget{Limit: 10, KV: kv, Now: func() time.Time { return now }}
	b3.Load(ctx)
	if _, n := b3.Count(); b3.Paused() || n != 0 {
		t.Fatalf("next day: paused=%v count=%d", b3.Paused(), n)
	}
}

func TestBudgetDisabled(t *testing.T) {
	ctx := context.Background()
	b := &Budget{Limit: 0, KV: memKV{}}
	b.Load(ctx)
	b.Add(ctx, 1_000_000)
	if b.Paused() {
		t.Error("disabled budget paused")
	}
	var nilB *Budget
	nilB.Add(ctx, 1)
	if nilB.Paused() {
		t.Error("nil budget paused")
	}
}
