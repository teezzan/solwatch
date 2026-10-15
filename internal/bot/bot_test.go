package bot

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teezzan/solwatch/internal/store"
)

func TestValidateAddress(t *testing.T) {
	valid := []string{
		"2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZ",
		"11111111111111111111111111111111", // System Program: 32 zero bytes
		"So11111111111111111111111111111111111111112",
	}
	for _, a := range valid {
		if err := ValidateAddress(a); err != nil {
			t.Errorf("%s: %v", a, err)
		}
	}
	invalid := []string{
		"",
		"hello",
		"2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHm0",  // '0' isn't base58
		"2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZZ", // 45 chars
		// a transaction signature (64 bytes), a classic paste mistake
		"5nNtjezQMYBHvgSQmoRmJPiXGsPAWmJPoGSa64xanqrauogiVzFyGQhKeFataHGXq51jR2hjbzNTkPUpP787HAmL",
	}
	for _, a := range invalid {
		if err := ValidateAddress(a); err == nil {
			t.Errorf("%q: expected error", a)
		}
	}
}

func TestCommands(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "bot.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	changes := 0
	b := &Bot{Store: st, OnChange: func() { changes++ }, MaxPerChat: 2}

	const addr = "2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZ"
	const addr2 = "HhgBQmx2Ja7hRRxVA53xVxY9jaWTt3GsNiVKcHHYn51F"

	steps := []struct {
		in, wantSubstr string
		changes        int
	}{
		{"/list", "not watching anything", 0},
		{"/watch", "usage", 0},
		{"/watch nope", "doesn't look like a Solana address", 0},
		{"/watch " + addr + " degen bot", "watching degen bot", 1},
		{"/watch@solwatch_bot " + addr + " jup trader", "updated label: jup trader", 1}, // relabel: no webhook sync
		{"/watch " + addr2, "watching HhgB…n51F", 2},
		{"/watch So11111111111111111111111111111111111111112", "limit of 2", 2},
		{"/list", "jup trader - " + addr, 2},
		{"/unwatch jup", "not watching jup", 2},
		{"/unwatch jup trader", "stopped watching jup trader", 3}, // multi-word label
		{"/unwatch " + addr2, "stopped watching", 4},
		{"/bogus", "unknown command", 4},
	}
	for _, s := range steps {
		got := b.Handle(ctx, 42, s.in)
		if !strings.Contains(got, s.wantSubstr) {
			t.Errorf("%q -> %q, want it to contain %q", s.in, got, s.wantSubstr)
		}
		if changes != s.changes {
			t.Errorf("%q: OnChange called %d times total, want %d", s.in, changes, s.changes)
		}
	}
}
