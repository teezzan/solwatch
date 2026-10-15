// Package bot implements the chat commands (/watch, /unwatch, /list), the
// demo guardrails around them, and the two front-ends that feed them:
// Telegram long polling and a stdin console for running without a token.
package bot

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/teezzan/solwatch/internal/alert"
	"github.com/teezzan/solwatch/internal/helius"
	"github.com/teezzan/solwatch/internal/notify"
	"github.com/teezzan/solwatch/internal/store"
	"github.com/teezzan/solwatch/internal/telegram"
)

// Store is the slice of persistence the commands need.
type Store interface {
	AddWatch(ctx context.Context, w store.Watch) (bool, error)
	RemoveWatch(ctx context.Context, chatID int64, addressOrLabel string) (bool, error)
	ListWatches(ctx context.Context, chatID int64) ([]store.Watch, error)
	CountWatches(ctx context.Context, chatID int64) (int, error)
	CountAddresses(ctx context.Context) (int, error)
	AddressWatched(ctx context.Context, addr string) (bool, error)
	ExpireWatches(ctx context.Context, cutoff time.Time) ([]store.Watch, error)
	RemoveAddress(ctx context.Context, addr string) ([]store.Watch, error)
	Block(ctx context.Context, addr string, until time.Time, reason string) error
	BlockedUntil(ctx context.Context, addr string) (time.Time, string, error)
	ChatsWithWatches(ctx context.Context) ([]int64, error)
}

// Accounts looks up an address on chain (Helius getAccountInfo).
type Accounts interface {
	GetAccountInfo(ctx context.Context, address string) (*helius.AccountInfo, error)
}

// Limits are the public-demo guardrails. Zero values disable a limit.
type Limits struct {
	MaxPerChat        int           // watches per chat
	MaxAddresses      int           // distinct addresses across all chats
	TTL               time.Duration // watches expire after this
	CommandsPerMinute int           // per chat
	WalletsOnly       bool          // refuse accounts not owned by the System Program
	HotPerMinute      int           // shown in help; enforced by pipeline.Breaker
	HotBlock          time.Duration // how long a hot address stays blocked
	DigestInterval    time.Duration // shown in help; enforced by notify.Digest
	DailyEventBudget  int64         // shown in help; enforced by budget.Budget
	RepoURL           string
	WriteupURL        string
}

type Bot struct {
	Store    Store
	Accounts Accounts    // optional; nil skips the on-chain check
	Notes    notify.Sink // for messages the bot sends unprompted (expiry, breaker)
	OnChange func()      // called after the watch set changes (triggers a webhook sync)
	Limits   Limits
	Log      *slog.Logger
	Now      func() time.Time
	Paused   func() bool // daily event budget spent

	rl rateLimiter
}

const (
	maxLabelLen   = 32
	systemProgram = "11111111111111111111111111111111"
)

func (b *Bot) now() time.Time {
	if b.Now != nil {
		return b.Now()
	}
	return time.Now()
}

func (b *Bot) log() *slog.Logger {
	if b.Log != nil {
		return b.Log
	}
	return slog.Default()
}

// Help is the /start and /help text. It spells out the demo limits so
// nobody is surprised when a watch disappears.
func (b *Bot) Help() string {
	l := b.Limits
	var sb strings.Builder
	sb.WriteString("solwatch: real-time Solana wallet alerts, powered by Helius webhooks.\n\n")
	sb.WriteString("/watch <address> [label] - start watching a wallet\n")
	sb.WriteString("/unwatch <address|label> - stop watching\n")
	sb.WriteString("/list - what this chat is watching\n\n")
	sb.WriteString("This is a public demo on a free Helius plan, so it has limits:\n")
	if l.MaxPerChat > 0 {
		fmt.Fprintf(&sb, "• up to %d wallets per chat", l.MaxPerChat)
		if l.MaxAddresses > 0 {
			fmt.Fprintf(&sb, " (%d across the whole demo)", l.MaxAddresses)
		}
		sb.WriteString("\n")
	}
	if l.TTL > 0 {
		fmt.Fprintf(&sb, "• watches expire after %s\n", human(l.TTL))
	}
	if l.DigestInterval > 0 {
		fmt.Fprintf(&sb, "• busy wallets get one grouped message every %s\n", human(l.DigestInterval))
	}
	if l.HotPerMinute > 0 {
		fmt.Fprintf(&sb, "• wallets doing more than %d transactions a minute are removed automatically\n", l.HotPerMinute)
	}
	if l.WalletsOnly {
		sb.WriteString("• wallets only: programs, token mints and pools are refused\n")
	}
	if l.DailyEventBudget > 0 {
		fmt.Fprintf(&sb, "• after %d transactions in a day (all chats together), alerts pause until 00:00 UTC\n", l.DailyEventBudget)
	}
	if l.RepoURL != "" || l.WriteupURL != "" {
		sb.WriteString("\n")
	}
	if l.RepoURL != "" {
		fmt.Fprintf(&sb, "Code: %s\n", l.RepoURL)
	}
	if l.WriteupURL != "" {
		fmt.Fprintf(&sb, "How it works: %s\n", l.WriteupURL)
	}
	return strings.TrimRight(sb.String(), "\n")
}

// Handle runs one command and returns the reply as plain text. An empty
// reply means "say nothing" (used when a chat is rate limited).
func (b *Bot) Handle(ctx context.Context, chatID int64, text string) string {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return b.Help()
	}
	if ok, warn := b.rl.allow(chatID, b.Limits.CommandsPerMinute, b.now()); !ok {
		if warn {
			return "you're sending commands too fast. try again in a minute."
		}
		return ""
	}
	cmd := strings.ToLower(fields[0])
	if i := strings.IndexByte(cmd, '@'); i > 0 {
		cmd = cmd[:i] // "/watch@solwatch_bot" in group chats
	}
	args := fields[1:]

	switch cmd {
	case "/start", "/help":
		return b.Help()
	case "/watch":
		return b.watch(ctx, chatID, args)
	case "/unwatch":
		if len(args) == 0 {
			return "usage: /unwatch <address|label>"
		}
		// Labels can contain spaces ("/watch <addr> jup trader"), so the
		// target is the rest of the line, not just the first word.
		target := strings.Join(args, " ")
		ok, err := b.Store.RemoveWatch(ctx, chatID, target)
		if err != nil {
			return "something went wrong, try again"
		}
		if !ok {
			return "you're not watching " + target
		}
		b.changed()
		return "stopped watching " + target
	case "/list":
		ws, err := b.Store.ListWatches(ctx, chatID)
		if err != nil {
			return "something went wrong, try again"
		}
		if len(ws) == 0 {
			return "you're not watching anything yet. try /watch <address> [label]"
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "watching %d address(es):", len(ws))
		for _, w := range ws {
			label := w.Label
			if label == "" {
				label = "(no label)"
			}
			fmt.Fprintf(&sb, "\n• %s - %s", label, w.Address)
			if b.Limits.TTL > 0 {
				left := w.CreatedAt.Add(b.Limits.TTL).Sub(b.now())
				fmt.Fprintf(&sb, " (expires in %s)", human(max(left, 0)))
			}
		}
		return sb.String()
	default:
		return "unknown command\n\n" + b.Help()
	}
}

func (b *Bot) watch(ctx context.Context, chatID int64, args []string) string {
	if len(args) == 0 {
		// Telegram turns "/watch" in any message into a tappable link that
		// sends the bare command, so this reply has to teach, not scold.
		return "send the address to watch after the command, plus an optional label:\n" +
			"/watch <address> [label]\n\n" +
			"e.g. /watch 2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZ jup trader"
	}
	addr := args[0]
	if err := ValidateAddress(addr); err != nil {
		return fmt.Sprintf("%q doesn't look like a Solana address: %v", addr, err)
	}
	label := strings.Join(args[1:], " ")
	if utf8.RuneCountInString(label) > maxLabelLen {
		return fmt.Sprintf("label is too long (max %d characters)", maxLabelLen)
	}
	name := label
	if name == "" {
		name = alert.Short(addr)
	}

	// Relabelling something this chat already watches skips every limit.
	mine, err := b.Store.ListWatches(ctx, chatID)
	if err != nil {
		return "something went wrong, try again"
	}
	for _, w := range mine {
		if w.Address == addr {
			if _, err := b.Store.AddWatch(ctx, store.Watch{ChatID: chatID, Address: addr, Label: label}); err != nil {
				return "something went wrong, try again"
			}
			return "updated label: " + name
		}
	}

	if until, reason, err := b.Store.BlockedUntil(ctx, addr); err != nil {
		return "something went wrong, try again"
	} else if until.After(b.now()) {
		return fmt.Sprintf("%s was removed automatically (%s). it can be watched again in %s.",
			alert.Short(addr), reason, human(until.Sub(b.now())))
	}
	if l := b.Limits.MaxPerChat; l > 0 && len(mine) >= l {
		return fmt.Sprintf("this demo allows %d watched wallets per chat. /unwatch one first.", l)
	}

	watched, err := b.Store.AddressWatched(ctx, addr)
	if err != nil {
		return "something went wrong, try again"
	}
	if !watched {
		if l := b.Limits.MaxAddresses; l > 0 {
			n, err := b.Store.CountAddresses(ctx)
			if err != nil {
				return "something went wrong, try again"
			}
			if n >= l {
				return fmt.Sprintf("the demo is full (%d wallets watched across all chats). try again later.", l)
			}
		}
		if msg := b.checkAccount(ctx, addr); msg != "" {
			return msg
		}
	}

	if _, err := b.Store.AddWatch(ctx, store.Watch{ChatID: chatID, Address: addr, Label: label}); err != nil {
		return "something went wrong, try again"
	}
	b.changed()
	reply := "👀 watching " + name + "."
	if d := b.Limits.DigestInterval; d > 0 {
		reply += fmt.Sprintf(" alerts are grouped and sent every %s.", human(d))
	} else {
		reply += " alerts start within a few seconds."
	}
	if b.Limits.TTL > 0 {
		reply += fmt.Sprintf(" this watch expires in %s.", human(b.Limits.TTL))
	}
	if b.Paused != nil && b.Paused() {
		reply += "\n\n" + pausedNote
	}
	return reply
}

// checkAccount refuses programs (always) and, with WalletsOnly, anything not
// owned by the System Program: token mints, pools and other program-owned
// accounts are the busiest addresses on chain and would burn the demo's
// credits in minutes. It returns a refusal message, or "" to allow.
func (b *Bot) checkAccount(ctx context.Context, addr string) string {
	if strings.HasPrefix(addr, "Sysvar") {
		return "that's a sysvar, not a wallet."
	}
	if b.Accounts == nil {
		return ""
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	info, err := b.Accounts.GetAccountInfo(cctx, addr)
	if err != nil {
		b.log().Warn("getAccountInfo failed", "address", addr, "err", err)
		return "couldn't check that address on chain right now. try again in a moment."
	}
	switch {
	case !info.Exists:
		return "" // a never-funded wallet is still a wallet
	case info.Executable:
		return "that's a program, not a wallet. programs see far too much traffic for this demo."
	case b.Limits.WalletsOnly && info.Owner != systemProgram:
		return "that account is owned by a program (" + alert.Short(info.Owner) + "), so it's probably a token mint, pool or similar. this demo only watches wallets."
	}
	return ""
}

func (b *Bot) changed() {
	if b.OnChange != nil {
		b.OnChange()
	}
}

func (b *Bot) note(ctx context.Context, chatID int64, text string) {
	if b.Notes == nil {
		return
	}
	if err := b.Notes.Send(ctx, chatID, notify.Plain(text)); err != nil {
		b.log().Warn("note failed", "chat_id", chatID, "err", err)
	}
}

// ExpireOnce removes watches older than the TTL and tells their chats.
func (b *Bot) ExpireOnce(ctx context.Context) {
	if b.Limits.TTL <= 0 {
		return
	}
	gone, err := b.Store.ExpireWatches(ctx, b.now().Add(-b.Limits.TTL))
	if err != nil {
		b.log().Error("expire watches", "err", err)
		return
	}
	for _, w := range gone {
		name := w.Label
		if name == "" {
			name = alert.Short(w.Address)
		}
		b.log().Info("watch expired", "chat_id", w.ChatID, "address", w.Address)
		b.note(ctx, w.ChatID, fmt.Sprintf("⏰ stopped watching %s: demo watches expire after %s. send /watch %s %s to keep going.",
			name, human(b.Limits.TTL), w.Address, w.Label))
	}
	if len(gone) > 0 {
		b.changed()
	}
}

// RunExpiry calls ExpireOnce every interval until ctx ends.
func (b *Bot) RunExpiry(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.ExpireOnce(ctx)
		}
	}
}

// Evict unwatches a hot address for every chat, blocks it for a while and
// says why. Wired to the pipeline's breaker.
func (b *Bot) Evict(ctx context.Context, addr string, perMinute int) {
	gone, err := b.Store.RemoveAddress(ctx, addr)
	if err != nil {
		b.log().Error("evict hot address", "address", addr, "err", err)
		return
	}
	reason := fmt.Sprintf("more than %d transactions a minute", b.Limits.HotPerMinute)
	if b.Limits.HotBlock > 0 {
		if err := b.Store.Block(ctx, addr, b.now().Add(b.Limits.HotBlock), reason); err != nil {
			b.log().Error("block hot address", "address", addr, "err", err)
		}
	}
	b.log().Warn("evicted hot address", "address", addr, "events_per_min", perMinute, "chats", len(gone))
	for _, w := range gone {
		name := w.Label
		if name == "" {
			name = alert.Short(w.Address)
		}
		b.note(ctx, w.ChatID, fmt.Sprintf("🔥 stopped watching %s: it did %d+ transactions in the last minute, more than this free demo can afford (limit %d/min). it can't be watched again for %s.",
			name, perMinute, b.Limits.HotPerMinute, human(b.Limits.HotBlock)))
	}
	if len(gone) > 0 {
		b.changed()
	}
}

const pausedNote = "⏸ demo daily budget reached, resumes 00:00 UTC. your watches are kept."

// BudgetPaused tells every chat with watches that alerts are paused for
// the rest of the UTC day. Wired to budget.Budget.OnPause.
func (b *Bot) BudgetPaused(ctx context.Context) {
	b.broadcast(ctx, pausedNote)
	b.log().Warn("daily event budget reached: alerts paused until 00:00 UTC")
	b.changed() // reconciler sees Paused() and deletes the webhook
}

// BudgetResumed tells chats alerts are back. Wired to OnResume.
func (b *Bot) BudgetResumed(ctx context.Context) {
	b.broadcast(ctx, "▶️ new UTC day: the demo budget has reset and alerts are back on.")
	b.log().Info("daily event budget reset: alerts resumed")
	b.changed() // recreate the webhook with the current watch list
}

func (b *Bot) broadcast(ctx context.Context, text string) {
	chats, err := b.Store.ChatsWithWatches(ctx)
	if err != nil {
		b.log().Error("list chats", "err", err)
		return
	}
	for _, c := range chats {
		b.note(ctx, c, text)
	}
}

// rateLimiter is a per-chat fixed window. It warns once per window.
type rateLimiter struct {
	mu    sync.Mutex
	chats map[int64]*rlWindow
}

type rlWindow struct {
	start  time.Time
	n      int
	warned bool
}

func (r *rateLimiter) allow(chatID int64, perMinute int, now time.Time) (ok, warn bool) {
	if perMinute <= 0 {
		return true, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.chats == nil {
		r.chats = map[int64]*rlWindow{}
	}
	w := r.chats[chatID]
	if w == nil || now.Sub(w.start) >= time.Minute {
		w = &rlWindow{start: now}
		r.chats[chatID] = w
	}
	w.n++
	if w.n <= perMinute {
		return true, false
	}
	if !w.warned {
		w.warned = true
		return false, true
	}
	return false, false
}

// human formats durations the way people say them: 24h, 60s, 23h12m.
func human(d time.Duration) string {
	d = d.Round(time.Second)
	switch {
	case d <= 0:
		return "0s"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d/time.Second))
	case d < time.Hour:
		if d%time.Minute == 0 {
			return fmt.Sprintf("%dm", int(d/time.Minute))
		}
		return fmt.Sprintf("%dm%ds", int(d/time.Minute), int(d%time.Minute/time.Second))
	default:
		h, m := int(d/time.Hour), int(d%time.Hour/time.Minute)
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%dm", h, m)
	}
}

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// ValidateAddress checks that s is base58 and decodes to a 32-byte public
// key. It catches typos and pasted signatures before we pay 100 credits to
// push them to Helius.
func ValidateAddress(s string) error {
	if len(s) < 32 || len(s) > 44 {
		return errors.New("wrong length")
	}
	n := new(big.Int)
	radix := big.NewInt(58)
	for _, r := range s {
		i := strings.IndexRune(base58Alphabet, r)
		if i < 0 {
			return fmt.Errorf("invalid character %q", r)
		}
		n.Mul(n, radix)
		n.Add(n, big.NewInt(int64(i)))
	}
	// Leading '1's are leading zero bytes.
	zeros := len(s) - len(strings.TrimLeft(s, "1"))
	if size := zeros + len(n.Bytes()); size != 32 {
		return fmt.Errorf("decodes to %d bytes, want 32", size)
	}
	return nil
}

// RunTelegram long-polls Telegram and answers commands until ctx ends.
func (b *Bot) RunTelegram(ctx context.Context, tg *telegram.Client) {
	log := b.log()
	var offset int64
	for ctx.Err() == nil {
		updates, err := tg.GetUpdates(ctx, offset, 30*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("telegram getUpdates failed", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}
		for _, u := range updates {
			offset = u.UpdateID + 1
			if u.Message == nil || !strings.HasPrefix(u.Message.Text, "/") {
				continue
			}
			cmd, _, _ := strings.Cut(u.Message.Text, " ")
			log.Info("command", "chat_id", u.Message.Chat.ID, "cmd", cmd)
			reply := b.Handle(ctx, u.Message.Chat.ID, u.Message.Text)
			if reply == "" {
				continue
			}
			if err := tg.SendMessage(ctx, u.Message.Chat.ID, html.EscapeString(reply)); err != nil {
				log.Warn("telegram reply failed", "chat_id", u.Message.Chat.ID, "err", err)
			}
		}
	}
}

// RunConsole reads commands from r (one per line) as chat chatID and
// writes replies to w. Lets you drive solwatch without Telegram.
func (b *Bot) RunConsole(ctx context.Context, r io.Reader, w io.Writer, chatID int64) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if ctx.Err() != nil {
			return
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fmt.Fprintln(w, "> "+line)
		if reply := b.Handle(ctx, chatID, line); reply != "" {
			fmt.Fprintln(w, reply)
		}
	}
}
