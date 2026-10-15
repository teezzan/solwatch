// Package bot implements the chat commands (/watch, /unwatch, /list) and
// the two front-ends that feed them: Telegram long polling and a stdin
// console for running without a bot token.
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
	"time"
	"unicode/utf8"

	"github.com/teezzan/solwatch/internal/alert"
	"github.com/teezzan/solwatch/internal/store"
	"github.com/teezzan/solwatch/internal/telegram"
)

// Store is the slice of persistence the commands need.
type Store interface {
	AddWatch(ctx context.Context, w store.Watch) (bool, error)
	RemoveWatch(ctx context.Context, chatID int64, addressOrLabel string) (bool, error)
	ListWatches(ctx context.Context, chatID int64) ([]store.Watch, error)
	CountWatches(ctx context.Context, chatID int64) (int, error)
}

type Bot struct {
	Store      Store
	OnChange   func() // called after the watch set changes (triggers a webhook sync)
	MaxPerChat int
}

const maxLabelLen = 32

const help = `solwatch watches Solana wallets and pings you when they move.

/watch <address> [label] - start watching an address
/unwatch <address|label> - stop watching
/list - show what you're watching`

// Handle runs one command and returns the reply as plain text.
func (b *Bot) Handle(ctx context.Context, chatID int64, text string) string {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return help
	}
	cmd := strings.ToLower(fields[0])
	if i := strings.IndexByte(cmd, '@'); i > 0 {
		cmd = cmd[:i] // "/watch@solwatch_bot" in group chats
	}
	args := fields[1:]

	switch cmd {
	case "/start", "/help":
		return help
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
		}
		return sb.String()
	default:
		return "unknown command\n\n" + help
	}
}

func (b *Bot) watch(ctx context.Context, chatID int64, args []string) string {
	if len(args) == 0 {
		return "usage: /watch <address> [label]"
	}
	addr := args[0]
	if err := ValidateAddress(addr); err != nil {
		return fmt.Sprintf("%q doesn't look like a Solana address: %v", addr, err)
	}
	label := strings.Join(args[1:], " ")
	if utf8.RuneCountInString(label) > maxLabelLen {
		return fmt.Sprintf("label is too long (max %d characters)", maxLabelLen)
	}

	if b.MaxPerChat > 0 {
		n, err := b.Store.CountWatches(ctx, chatID)
		if err != nil {
			return "something went wrong, try again"
		}
		if n >= b.MaxPerChat {
			// Relabelling an existing watch is still fine; AddWatch tells us.
			ws, _ := b.Store.ListWatches(ctx, chatID)
			if !containsAddr(ws, addr) {
				return fmt.Sprintf("you're at the limit of %d watched addresses. /unwatch one first", b.MaxPerChat)
			}
		}
	}

	created, err := b.Store.AddWatch(ctx, store.Watch{ChatID: chatID, Address: addr, Label: label})
	if err != nil {
		return "something went wrong, try again"
	}
	name := label
	if name == "" {
		name = alert.Short(addr)
	}
	if !created {
		return "updated label: " + name
	}
	b.changed()
	return "👀 watching " + name + ". alerts start within a few seconds."
}

func (b *Bot) changed() {
	if b.OnChange != nil {
		b.OnChange()
	}
}

func containsAddr(ws []store.Watch, addr string) bool {
	for _, w := range ws {
		if w.Address == addr {
			return true
		}
	}
	return false
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
func (b *Bot) RunTelegram(ctx context.Context, tg *telegram.Client, log *slog.Logger) {
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
		fmt.Fprintln(w, b.Handle(ctx, chatID, line))
	}
}
