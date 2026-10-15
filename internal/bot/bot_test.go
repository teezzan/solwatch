package bot

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/teezzan/solwatch/internal/helius"
	"github.com/teezzan/solwatch/internal/notify"

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
	b := &Bot{Store: st, OnChange: func() { changes++ }, Limits: Limits{MaxPerChat: 2}}

	const addr = "2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZ"
	const addr2 = "HhgBQmx2Ja7hRRxVA53xVxY9jaWTt3GsNiVKcHHYn51F"

	steps := []struct {
		in, wantSubstr string
		changes        int
	}{
		{"/list", "not watching anything", 0},
		{"/watch", "e.g. /watch ", 0}, // bare command, e.g. a tapped link
		{"/watch nope", "doesn't look like a Solana address", 0},
		{"/watch " + addr + " degen bot", "watching degen bot", 1},
		{"/watch@solwatch_bot " + addr + " jup trader", "updated label: jup trader", 1}, // relabel: no webhook sync
		{"/watch " + addr2, "watching HhgB…n51F", 2},
		{"/watch So11111111111111111111111111111111111111112", "2 watched wallets per chat", 2},
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

// Real mainnet addresses with the account info Helius returned for them.
var chain = map[string]*helius.AccountInfo{
	"2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZ": {Exists: true, Owner: "11111111111111111111111111111111"},
	"HhgBQmx2Ja7hRRxVA53xVxY9jaWTt3GsNiVKcHHYn51F": {Exists: true, Owner: "11111111111111111111111111111111"},
	"GHnWxdj8qg2trFLHD5YbjAARVzahbjCghjLiFyMcsoBH": {Exists: true, Owner: "11111111111111111111111111111111"},
	"7crqCyp2x65GJPee3yCovrx2NgDVByY3YJzxRMoieuGf": {Exists: true, Owner: "11111111111111111111111111111111"},
	"JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4":  {Exists: true, Executable: true, Owner: "BPFLoaderUpgradeab1e11111111111111111111111"},
	"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {Exists: true, Owner: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
}

type fakeChain struct{ calls int }

func (f *fakeChain) GetAccountInfo(_ context.Context, a string) (*helius.AccountInfo, error) {
	f.calls++
	if i, ok := chain[a]; ok {
		return i, nil
	}
	return &helius.AccountInfo{}, nil // unfunded
}

type notes struct{ got map[int64][]string }

func (n *notes) Send(_ context.Context, chatID int64, m notify.Message) error {
	if n.got == nil {
		n.got = map[int64][]string{}
	}
	n.got[chatID] = append(n.got[chatID], m.Text)
	return nil
}

func newDemoBot(t *testing.T) (*Bot, *fakeChain, *notes, *time.Time) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "demo.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now()
	fc, n := &fakeChain{}, &notes{}
	b := &Bot{
		Store: st, Accounts: fc, Notes: n,
		Limits: Limits{
			MaxPerChat: 3, MaxAddresses: 4, TTL: 24 * time.Hour, CommandsPerMinute: 20,
			WalletsOnly: true, HotPerMinute: 30, HotBlock: 24 * time.Hour, DigestInterval: time.Minute,
			RepoURL: "https://github.com/teezzan/solwatch", WriteupURL: "https://blog.example/post",
		},
		Now: func() time.Time { return now },
	}
	return b, fc, n, &now
}

const (
	w1 = "2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZ"
	w2 = "HhgBQmx2Ja7hRRxVA53xVxY9jaWTt3GsNiVKcHHYn51F"
	w3 = "GHnWxdj8qg2trFLHD5YbjAARVzahbjCghjLiFyMcsoBH"
	w4 = "7crqCyp2x65GJPee3yCovrx2NgDVByY3YJzxRMoieuGf"
)

func TestHelpExplainsDemo(t *testing.T) {
	b, _, _, _ := newDemoBot(t)
	h := b.Handle(context.Background(), 1, "/start")
	for _, want := range []string{"/watch <address>", "up to 3 wallets per chat (4 across the whole demo)", "expire after 24h",
		"every 1m", "more than 30 transactions a minute", "wallets only", "https://github.com/teezzan/solwatch", "https://blog.example/post"} {
		if !strings.Contains(h, want) {
			t.Errorf("help missing %q:\n%s", want, h)
		}
	}
}

func TestWatchRefusesProgramsAndMints(t *testing.T) {
	ctx := context.Background()
	b, _, _, _ := newDemoBot(t)
	if got := b.Handle(ctx, 1, "/watch JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"); !strings.Contains(got, "that's a program") {
		t.Errorf("program: %q", got)
	}
	if got := b.Handle(ctx, 1, "/watch EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v usdc"); !strings.Contains(got, "only watches wallets") {
		t.Errorf("mint: %q", got)
	}
	if got := b.Handle(ctx, 1, "/watch SysvarC1ock11111111111111111111111111111111"); !strings.Contains(got, "sysvar") {
		t.Errorf("sysvar: %q", got)
	}
	// An unfunded address is allowed: it's a wallet that hasn't been used yet.
	if got := b.Handle(ctx, 1, "/watch AAAAhaR1BKzXHzTo9TPYx5ZkxJDXjMeVKqB8bYnS5WGU fresh"); !strings.Contains(got, "watching fresh") {
		t.Errorf("unfunded: %q", got)
	}
}

func TestPerChatAndGlobalCaps(t *testing.T) {
	ctx := context.Background()
	b, fc, _, _ := newDemoBot(t)
	for _, a := range []string{w1, w2, w3} {
		if got := b.Handle(ctx, 1, "/watch "+a); !strings.Contains(got, "watching") {
			t.Fatalf("watch %s: %q", a, got)
		}
	}
	if got := b.Handle(ctx, 1, "/watch "+w4); !strings.Contains(got, "3 watched wallets per chat") {
		t.Errorf("4th watch in one chat: %q", got)
	}
	// Relabel at the limit still works.
	if got := b.Handle(ctx, 1, "/watch "+w1+" renamed"); got != "updated label: renamed" {
		t.Errorf("relabel at limit: %q", got)
	}
	// Chat 2 can share an existing address without counting against the
	// global cap or hitting RPC again.
	before := fc.calls
	if got := b.Handle(ctx, 2, "/watch "+w1); !strings.Contains(got, "watching") || fc.calls != before {
		t.Errorf("shared watch: %q (rpc calls %d -> %d)", got, before, fc.calls)
	}
	if got := b.Handle(ctx, 2, "/watch "+w4); !strings.Contains(got, "watching") {
		t.Errorf("4th distinct address: %q", got)
	}
	// 4 distinct addresses watched: the global cap of 4 is reached.
	if got := b.Handle(ctx, 3, "/watch AAAAhaR1BKzXHzTo9TPYx5ZkxJDXjMeVKqB8bYnS5WGU"); !strings.Contains(got, "the demo is full") {
		t.Errorf("global cap: %q", got)
	}
}

func TestWatchesExpire(t *testing.T) {
	ctx := context.Background()
	b, _, n, now := newDemoBot(t)
	changes := 0
	b.OnChange = func() { changes++ }
	b.Handle(ctx, 1, "/watch "+w1+" jup trader")
	// created_at is stored in whole seconds, so this is 24h or 23h59m.
	if got := b.Handle(ctx, 1, "/list"); !strings.Contains(got, "(expires in 24h)") && !strings.Contains(got, "(expires in 23h59m)") {
		t.Errorf("/list: %q", got)
	}

	b.ExpireOnce(ctx) // too early
	if len(n.got[1]) != 0 {
		t.Fatalf("expired early: %v", n.got)
	}
	*now = now.Add(24*time.Hour + time.Minute)
	b.ExpireOnce(ctx)
	if len(n.got[1]) != 1 || !strings.Contains(n.got[1][0], "⏰ stopped watching jup trader") || !strings.Contains(n.got[1][0], "/watch "+w1+" jup trader") {
		t.Errorf("expiry note: %v", n.got[1])
	}
	if changes != 2 { // watch + expiry
		t.Errorf("OnChange calls = %d, want 2", changes)
	}
	if got := b.Handle(ctx, 1, "/list"); !strings.Contains(got, "not watching anything") {
		t.Errorf("/list after expiry: %q", got)
	}
}

func TestEvictHotAddress(t *testing.T) {
	ctx := context.Background()
	b, _, n, now := newDemoBot(t)
	b.Handle(ctx, 1, "/watch "+w1+" bot")
	b.Handle(ctx, 2, "/watch "+w1)

	b.Evict(ctx, w1, 31)
	for _, chat := range []int64{1, 2} {
		if len(n.got[chat]) != 1 || !strings.Contains(n.got[chat][0], "🔥 stopped watching") || !strings.Contains(n.got[chat][0], "limit 30/min") {
			t.Errorf("chat %d notes: %v", chat, n.got[chat])
		}
	}
	if got := b.Handle(ctx, 1, "/watch "+w1); !strings.Contains(got, "removed automatically") {
		t.Errorf("re-watch while blocked: %q", got)
	}
	*now = now.Add(25 * time.Hour)
	if got := b.Handle(ctx, 1, "/watch "+w1); !strings.Contains(got, "watching") {
		t.Errorf("re-watch after block: %q", got)
	}
}

func TestCommandRateLimit(t *testing.T) {
	ctx := context.Background()
	b, _, _, now := newDemoBot(t)
	b.Limits.CommandsPerMinute = 3
	for i := 0; i < 3; i++ {
		if got := b.Handle(ctx, 1, "/list"); got == "" {
			t.Fatalf("command %d was limited", i)
		}
	}
	if got := b.Handle(ctx, 1, "/list"); !strings.Contains(got, "too fast") {
		t.Errorf("4th command: %q", got)
	}
	if got := b.Handle(ctx, 1, "/list"); got != "" {
		t.Errorf("5th command should be silent, got %q", got)
	}
	if got := b.Handle(ctx, 2, "/list"); got == "" {
		t.Error("another chat was limited")
	}
	*now = now.Add(time.Minute)
	if got := b.Handle(ctx, 1, "/list"); got == "" || strings.Contains(got, "too fast") {
		t.Errorf("after a minute: %q", got)
	}
}

func TestHuman(t *testing.T) {
	for d, want := range map[time.Duration]string{
		24 * time.Hour: "24h", time.Minute: "1m", 45 * time.Second: "45s",
		23*time.Hour + 12*time.Minute: "23h12m", 90 * time.Second: "1m30s", -time.Second: "0s",
	} {
		if got := human(d); got != want {
			t.Errorf("human(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestBudgetPauseBroadcasts(t *testing.T) {
	ctx := context.Background()
	b, _, n, _ := newDemoBot(t)
	b.Limits.DailyEventBudget = 25000
	changes := 0
	b.OnChange = func() { changes++ }
	b.Handle(ctx, 1, "/watch "+w1)
	b.Handle(ctx, 2, "/watch "+w2)
	changes = 0

	b.BudgetPaused(ctx)
	for _, chat := range []int64{1, 2} {
		if len(n.got[chat]) != 1 || !strings.Contains(n.got[chat][0], "demo daily budget reached, resumes 00:00 UTC") {
			t.Errorf("chat %d: %v", chat, n.got[chat])
		}
	}
	if changes != 1 {
		t.Errorf("OnChange calls = %d, want 1 (to delete the webhook)", changes)
	}

	paused := true
	b.Paused = func() bool { return paused }
	if got := b.Handle(ctx, 3, "/watch "+w3); !strings.Contains(got, "watching") || !strings.Contains(got, "resumes 00:00 UTC") {
		t.Errorf("watch while paused: %q", got)
	}
	if h := b.Help(); !strings.Contains(h, "after 25000 transactions in a day") {
		t.Errorf("help: %s", h)
	}

	paused = false
	b.BudgetResumed(ctx)
	if got := n.got[1][len(n.got[1])-1]; !strings.Contains(got, "alerts are back") {
		t.Errorf("resume note: %q", got)
	}
}
