package notify

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/teezzan/solwatch/internal/alert"
)

type memSink struct {
	mu   sync.Mutex
	sent map[int64][]Message
}

func (m *memSink) Send(_ context.Context, chatID int64, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sent == nil {
		m.sent = map[int64][]Message{}
	}
	m.sent[chatID] = append(m.sent[chatID], msg)
	return nil
}

func a(addr, label, summary, sig string) alert.Alert {
	return alert.Alert{Address: addr, Label: label, Summary: summary, Signature: sig, Kind: alert.KindOf(summary)}
}

func TestDigestGroupsPerChatAndAddress(t *testing.T) {
	ctx := context.Background()
	sink := &memSink{}
	d := &Digest{Sink: sink, Interval: time.Minute, MaxLines: 5}

	// Chat 1 / wallet A: a burst of 7 (summaries from the live Telegram run).
	burst := []string{
		"swapped 19,808.97 BOTTLE → 0.01331 SOL on Jupiter",
		"swapped 28,505.59 BOTTLE → 0.01945 SOL on Jupiter",
		"sent 0.00003028 SOL to uncN…yn3M",
		"sent 0.00009174 SOL to uncN…yn3M",
		"received 0.002917 SOL",
		"swapped 0.0003 SOL → 1,970,826.16 MINNIE on Jupiter",
		"swapped 0.0002 SOL → 1,174,837.64 CLAWDM on Jupiter",
	}
	for i, s := range burst {
		d.Notify(ctx, 1, a("A", "jup trader", s, "sig"+string(rune('a'+i))))
	}
	// Chat 1 / wallet B: one event. Chat 2 / wallet A: one event.
	d.Notify(ctx, 1, a("B", "whale", "received 16.2617 SOL from 7Qjx…PxEi", "sigB"))
	d.Notify(ctx, 2, a("A", "other label", "sent 1 SOL to x", "sig2"))

	d.Flush(ctx)

	if got := len(sink.sent[1]); got != 2 {
		t.Fatalf("chat 1 got %d messages, want 2 (one digest, one single)", got)
	}
	dig := sink.sent[1][0]
	wantHead := "🔔 jup trader · 7 events in the last minute: 4 swaps, 2 sends, 1 receive"
	if !strings.HasPrefix(dig.Text, wantHead+"\n") {
		t.Errorf("digest head:\n%s\nwant prefix:\n%s", dig.Text, wantHead)
	}
	if strings.Count(dig.Text, "\n• ") != 5 || !strings.HasSuffix(dig.Text, "\n+2 more") {
		t.Errorf("digest should show 5 lines then +2 more:\n%s", dig.Text)
	}
	if !strings.Contains(dig.HTML, `<b>jup trader</b>`) || !strings.Contains(dig.HTML, `<a href="https://solscan.io/tx/siga">tx</a>`) {
		t.Errorf("digest HTML:\n%s", dig.HTML)
	}

	single := sink.sent[1][1]
	if single.Text != "🔔 whale: received 16.2617 SOL from 7Qjx…PxEi · https://solscan.io/tx/sigB" {
		t.Errorf("single alert rendered as %q", single.Text)
	}
	if len(sink.sent[2]) != 1 || !strings.Contains(sink.sent[2][0].Text, "other label") {
		t.Errorf("chat 2: %+v", sink.sent[2])
	}

	// Buffer is empty after a flush.
	d.Flush(ctx)
	if len(sink.sent[1]) != 2 {
		t.Error("second flush re-sent alerts")
	}
}

func TestDigestEscapesHTML(t *testing.T) {
	m := Render([]alert.Alert{
		a("A", "<i>x</i>", "sent 1 SOL", "s1"),
		a("A", "<i>x</i>", "sent 2 SOL", "s2"),
	}, "", 30*time.Second, 5)
	if strings.Contains(m.HTML, "<i>") || !strings.Contains(m.HTML, "2 events in the last 30s: 2 sends") {
		t.Errorf("HTML = %s", m.HTML)
	}
}

func TestWindowPhrase(t *testing.T) {
	for d, want := range map[time.Duration]string{
		time.Minute:      "the last minute",
		5 * time.Minute:  "the last 5 minutes",
		90 * time.Second: "the last 90s",
		10 * time.Second: "the last 10s",
	} {
		if got := windowPhrase(d); got != want {
			t.Errorf("windowPhrase(%v) = %q, want %q", d, got, want)
		}
	}
}
