package notify

import (
	"context"
	"fmt"
	"html"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/teezzan/solwatch/internal/alert"
)

// Digest buffers alerts per (chat, address) and sends one message per
// group every Interval. A busy wallet produced 20 separate Telegram
// messages in 8 minutes during testing; grouped, that's a handful.
//
// A group holding a single alert is sent as the normal one-line alert.
type Digest struct {
	Sink     Sink
	Explorer string
	Interval time.Duration
	MaxLines int // compact lines per digest before "+N more" (default 5)
	Log      *slog.Logger

	mu     sync.Mutex
	groups map[groupKey][]alert.Alert
	order  []groupKey // first-seen order, so digests go out predictably
}

type groupKey struct {
	chatID  int64
	address string
}

func (d *Digest) Notify(_ context.Context, chatID int64, a alert.Alert) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.groups == nil {
		d.groups = map[groupKey][]alert.Alert{}
	}
	k := groupKey{chatID, a.Address}
	if _, ok := d.groups[k]; !ok {
		d.order = append(d.order, k)
	}
	d.groups[k] = append(d.groups[k], a)
	return nil
}

// Run flushes every Interval until ctx is done. It does not flush on exit;
// call Flush with a fresh context during shutdown.
func (d *Digest) Run(ctx context.Context) {
	t := time.NewTicker(d.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.Flush(ctx)
		}
	}
}

// Flush sends everything buffered so far.
func (d *Digest) Flush(ctx context.Context) {
	d.mu.Lock()
	groups, order := d.groups, d.order
	d.groups, d.order = nil, nil
	d.mu.Unlock()

	for _, k := range order {
		m := Render(groups[k], d.Explorer, d.Interval, d.maxLines())
		if err := d.Sink.Send(ctx, k.chatID, m); err != nil && d.Log != nil {
			d.Log.Error("digest send failed", "chat_id", k.chatID, "address", k.address, "alerts", len(groups[k]), "err", err)
		} else if d.Log != nil {
			d.Log.Info("digest sent", "chat_id", k.chatID, "address", k.address, "alerts", len(groups[k]))
		}
	}
}

func (d *Digest) maxLines() int {
	if d.MaxLines <= 0 {
		return 5
	}
	return d.MaxLines
}

// Render turns one group of alerts (same chat, same address) into a
// message. One alert renders as the usual single line.
func Render(alerts []alert.Alert, explorer string, window time.Duration, maxLines int) Message {
	if len(alerts) == 1 {
		return Single(alerts[0], explorer)
	}
	label := alerts[0].Label
	head := fmt.Sprintf("%d events in %s: %s", len(alerts), windowPhrase(window), kindCounts(alerts))

	var text, htm strings.Builder
	fmt.Fprintf(&text, "🔔 %s · %s", label, head)
	fmt.Fprintf(&htm, "🔔 <b>%s</b> · %s", html.EscapeString(label), html.EscapeString(head))
	shown := min(len(alerts), maxLines)
	for _, a := range alerts[:shown] {
		u := fmt.Sprintf(explorerOr(explorer), a.Signature)
		fmt.Fprintf(&text, "\n• %s · %s", a.Summary, u)
		fmt.Fprintf(&htm, "\n• %s · <a href=\"%s\">tx</a>", html.EscapeString(a.Summary), html.EscapeString(u))
	}
	if rest := len(alerts) - shown; rest > 0 {
		fmt.Fprintf(&text, "\n+%d more", rest)
		fmt.Fprintf(&htm, "\n+%d more", rest)
	}
	return Message{Text: text.String(), HTML: htm.String()}
}

func explorerOr(e string) string {
	if e == "" {
		return "https://solscan.io/tx/%s"
	}
	return e
}

var kindOrder = []struct{ kind, one, many string }{
	{"swap", "swap", "swaps"},
	{"send", "send", "sends"},
	{"receive", "receive", "receives"},
	{"other", "other", "other"},
}

func kindCounts(alerts []alert.Alert) string {
	n := map[string]int{}
	for _, a := range alerts {
		k := a.Kind
		if k == "" {
			k = alert.KindOf(a.Summary)
		}
		n[k]++
	}
	var parts []string
	for _, k := range kindOrder {
		switch c := n[k.kind]; {
		case c == 1:
			parts = append(parts, "1 "+k.one)
		case c > 1:
			parts = append(parts, fmt.Sprintf("%d %s", c, k.many))
		}
	}
	return strings.Join(parts, ", ")
}

func windowPhrase(d time.Duration) string {
	switch {
	case d == time.Minute:
		return "the last minute"
	case d > time.Minute && d%time.Minute == 0:
		return fmt.Sprintf("the last %d minutes", int(d/time.Minute))
	default:
		return fmt.Sprintf("the last %ds", int(d.Round(time.Second)/time.Second))
	}
}
