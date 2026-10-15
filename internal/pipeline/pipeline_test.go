package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/teezzan/solwatch/internal/alert"
	"github.com/teezzan/solwatch/internal/helius"
	"github.com/teezzan/solwatch/internal/store"
)

type recorder struct{ got []alert.Alert }

func (r *recorder) Notify(_ context.Context, _ int64, a alert.Alert) error {
	r.got = append(r.got, a)
	return nil
}

type fakeDAS struct{ calls int }

func (f *fakeDAS) TokenMetadata(_ context.Context, mints []string) (map[string]helius.TokenMeta, error) {
	f.calls++
	out := map[string]helius.TokenMeta{}
	for _, m := range mints {
		if m == "G6kNgzAurBQqppvRYkEhTLUJ71JhwCsiJnrwHjqnpump" {
			out[m] = helius.TokenMeta{Mint: m, Symbol: "GROK", Decimals: 6}
		}
	}
	return out, nil
}

func TestProcessDedupesAndResolvesSymbols(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "p.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	st.AddWatch(ctx, store.Watch{ChatID: 7, Address: "2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZ", Label: "degen"})

	b, _ := os.ReadFile(filepath.Join("..", "..", "testdata", "payloads", "swap_sol_to_token.json"))
	var txs []helius.EnhancedTransaction
	if err := json.Unmarshal(b, &txs); err != nil {
		t.Fatal(err)
	}

	rec := &recorder{}
	das := &fakeDAS{}
	p := &Processor{Store: st, Tokens: das, Notifier: rec, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	// Helius may deliver the same event more than once.
	p.Process(ctx, &txs[0])
	p.Process(ctx, &txs[0])

	if len(rec.got) != 1 {
		t.Fatalf("got %d alerts, want exactly 1", len(rec.got))
	}
	want := "swapped 0.08221 SOL → 828,678.81 GROK on Jupiter"
	if a := rec.got[0]; a.Label != "degen" || a.Summary != want {
		t.Errorf("alert = %+v", a)
	}

	if das.calls != 1 {
		t.Fatalf("DAS calls = %d, want 1", das.calls)
	}

	// Both mints are now cached, including SPCX… which DAS had no symbol
	// for, so a second transaction with the same mints makes no DAS call.
	das2 := &fakeDAS{}
	p.Tokens = das2
	tx2 := txs[0]
	tx2.Signature = "another-signature"
	p.Process(ctx, &tx2)
	if das2.calls != 0 {
		t.Fatalf("DAS calls for second tx = %d, want 0", das2.calls)
	}
	if got := rec.got[len(rec.got)-1].Summary; got != want {
		t.Errorf("second alert = %q", got)
	}
}

func TestProcessIgnoresUnwatched(t *testing.T) {
	ctx := context.Background()
	st, _ := store.Open(filepath.Join(t.TempDir(), "p.db"))
	defer st.Close()
	b, _ := os.ReadFile(filepath.Join("..", "..", "testdata", "payloads", "transfer_sol.json"))
	var txs []helius.EnhancedTransaction
	json.Unmarshal(b, &txs)

	rec := &recorder{}
	p := &Processor{Store: st, Notifier: rec, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p.Process(ctx, &txs[0])
	if len(rec.got) != 0 {
		t.Fatalf("alerted with no watchers: %+v", rec.got)
	}
}

func TestProcessTripsBreaker(t *testing.T) {
	ctx := context.Background()
	st, _ := store.Open(filepath.Join(t.TempDir(), "p.db"))
	defer st.Close()
	const addr = "2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZ"
	st.AddWatch(ctx, store.Watch{ChatID: 7, Address: addr, Label: "degen"})

	b, _ := os.ReadFile(filepath.Join("..", "..", "testdata", "payloads", "swap_sol_to_token.json"))
	var txs []helius.EnhancedTransaction
	json.Unmarshal(b, &txs)

	var hot []string
	rec := &recorder{}
	p := &Processor{
		Store: st, Notifier: rec, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Breaker: &Breaker{Max: 2},
		OnHot:   func(_ context.Context, a string, n int) { hot = append(hot, a) },
	}
	for i := 0; i < 4; i++ {
		tx := txs[0]
		tx.Signature = fmt.Sprintf("sig-%d", i)
		p.Process(ctx, &tx)
	}
	if len(hot) != 1 || hot[0] != addr {
		t.Fatalf("OnHot calls = %v, want exactly one for %s", hot, addr)
	}
	// Events 1 and 2 alert, event 3 trips (no alert), event 4 is over the
	// limit but the breaker already fired; in production the watch is gone
	// by then. It still alerts here because the fake OnHot doesn't unwatch.
	if len(rec.got) != 3 {
		t.Errorf("alerts = %d, want 3", len(rec.got))
	}
}
