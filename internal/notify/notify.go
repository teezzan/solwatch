// Package notify delivers alerts. The pipeline only knows the Notifier
// interface; a Sink is where rendered messages end up (Telegram or a
// writer), and Digest sits in between to group bursts.
package notify

import (
	"context"
	"fmt"
	"html"
	"io"
	"sync"

	"github.com/teezzan/solwatch/internal/alert"
	"github.com/teezzan/solwatch/internal/telegram"
)

// Notifier accepts alerts from the pipeline.
type Notifier interface {
	Notify(ctx context.Context, chatID int64, a alert.Alert) error
}

// Message is one outgoing chat message in both renderings.
type Message struct {
	Text string // plain text, for stdout
	HTML string // Telegram HTML parse mode
}

// Plain wraps a plain-text note (bot notices, expiry messages) as a Message.
func Plain(s string) Message { return Message{Text: s, HTML: html.EscapeString(s)} }

// Sink delivers a rendered message to a chat.
type Sink interface {
	Send(ctx context.Context, chatID int64, m Message) error
}

// Telegram sends messages through the Bot API.
type Telegram struct{ Client *telegram.Client }

func (t *Telegram) Send(ctx context.Context, chatID int64, m Message) error {
	return t.Client.SendMessage(ctx, chatID, m.HTML)
}

// Writer prints messages as lines, e.g. to stdout. It's what runs when no
// Telegram token is configured.
type Writer struct {
	W  io.Writer
	mu sync.Mutex
}

func (w *Writer) Send(_ context.Context, chatID int64, m Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := fmt.Fprintf(w.W, "[chat %d] %s\n", chatID, m.Text)
	return err
}

// Immediate sends every alert as its own message.
type Immediate struct {
	Sink     Sink
	Explorer string
}

func (n *Immediate) Notify(ctx context.Context, chatID int64, a alert.Alert) error {
	return n.Sink.Send(ctx, chatID, Single(a, n.Explorer))
}

// Single renders one alert as one line.
func Single(a alert.Alert, explorer string) Message {
	return Message{Text: a.Text(explorer), HTML: a.HTML(explorer)}
}
