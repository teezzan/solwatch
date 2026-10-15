package store

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestWatchLifecycle(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	created, err := s.AddWatch(ctx, Watch{ChatID: 1, Address: "A", Label: "alice"})
	if err != nil || !created {
		t.Fatalf("first add: created=%v err=%v", created, err)
	}
	// Re-adding relabels instead of duplicating.
	created, err = s.AddWatch(ctx, Watch{ChatID: 1, Address: "A", Label: "alice2"})
	if err != nil || created {
		t.Fatalf("relabel: created=%v err=%v", created, err)
	}
	s.AddWatch(ctx, Watch{ChatID: 2, Address: "A", Label: "shared"})
	s.AddWatch(ctx, Watch{ChatID: 2, Address: "B"})

	addrs, _ := s.Addresses(ctx)
	if !reflect.DeepEqual(addrs, []string{"A", "B"}) {
		t.Errorf("Addresses() = %v", addrs)
	}
	ws, _ := s.ListWatches(ctx, 1)
	if len(ws) != 1 || ws[0].Label != "alice2" {
		t.Errorf("ListWatches(1) = %+v", ws)
	}
	got, _ := s.WatchersOf(ctx, map[string]struct{}{"A": {}, "Z": {}})
	if len(got) != 2 {
		t.Errorf("WatchersOf(A) = %+v, want both chats", got)
	}

	// Remove by label, then by address.
	if ok, _ := s.RemoveWatch(ctx, 1, "alice2"); !ok {
		t.Error("remove by label failed")
	}
	if ok, _ := s.RemoveWatch(ctx, 2, "B"); !ok {
		t.Error("remove by address failed")
	}
	if ok, _ := s.RemoveWatch(ctx, 2, "nope"); ok {
		t.Error("removing a missing watch reported success")
	}
	addrs, _ = s.Addresses(ctx)
	if !reflect.DeepEqual(addrs, []string{"A"}) {
		t.Errorf("Addresses() after removals = %v", addrs)
	}
}

func TestMarkSeenIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	if fresh, err := s.MarkSeen(ctx, "sig1"); err != nil || !fresh {
		t.Fatalf("first MarkSeen: fresh=%v err=%v", fresh, err)
	}
	if fresh, _ := s.MarkSeen(ctx, "sig1"); fresh {
		t.Fatal("second MarkSeen reported fresh")
	}
	if n, _ := s.PruneSeen(ctx, -time.Hour); n != 1 {
		t.Fatalf("PruneSeen removed %d rows, want 1", n)
	}
	if fresh, _ := s.MarkSeen(ctx, "sig1"); !fresh {
		t.Fatal("signature should be fresh after prune")
	}
}

func TestKVAndSymbols(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	if v, _ := s.Get(ctx, "k"); v != "" {
		t.Fatalf("unset key = %q", v)
	}
	s.Set(ctx, "k", "v1")
	s.Set(ctx, "k", "v2")
	if v, _ := s.Get(ctx, "k"); v != "v2" {
		t.Fatalf("Get = %q", v)
	}
	s.Delete(ctx, "k")
	if v, _ := s.Get(ctx, "k"); v != "" {
		t.Fatalf("after Delete = %q", v)
	}

	s.PutSymbols(ctx, map[string]string{"mint1": "AAA"})
	got, _ := s.Symbols(ctx, []string{"mint1", "mint2"})
	if !reflect.DeepEqual(got, map[string]string{"mint1": "AAA"}) {
		t.Fatalf("Symbols = %v", got)
	}
}
