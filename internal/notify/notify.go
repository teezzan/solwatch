// Package notify delivers alerts. The pipeline only knows the Notifier
// interface, so Telegram is one implementation and stdout is another.
package notify

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/teezzan/solwatch/internal/alert"
	"github.com/teezzan/solwatch/internal/telegram"
)

type Notifier interface {
	Notify(ctx context.Context, chatID int64, a alert.Alert) error
}

// Telegram sends alerts as HTML messages through the Bot API.
type Telegram struct {
	Client   *telegram.Client
	Explorer string // fmt pattern for tx links, e.g. "https://solscan.io/tx/%s"
}

func (t *Telegram) Notify(ctx context.Context, chatID int64, a alert.Alert) error {
	return t.Client.SendMessage(ctx, chatID, a.HTML(t.Explorer))
}

// Writer prints alerts as plain text lines, e.g. to stdout. It's what runs
// when no Telegram token is configured, and what the end-to-end check uses.
type Writer struct {
	W        io.Writer
	Explorer string
	Log      *slog.Logger

	mu sync.Mutex
}

func (w *Writer) Notify(_ context.Context, chatID int64, a alert.Alert) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.Log != nil {
		w.Log.Info("alert", "chat_id", chatID, "address", a.Address, "signature", a.Signature, "summary", a.Summary)
	}
	_, err := fmt.Fprintf(w.W, "[chat %d] %s\n", chatID, a.Text(w.Explorer))
	return err
}
