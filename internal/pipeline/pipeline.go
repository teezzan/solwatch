// Package pipeline turns queued webhook transactions into alerts:
// dedupe, route to watchers, resolve token symbols, format, notify.
package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/teezzan/solwatch/internal/alert"
	"github.com/teezzan/solwatch/internal/helius"
	"github.com/teezzan/solwatch/internal/notify"
	"github.com/teezzan/solwatch/internal/store"
)

type Store interface {
	MarkSeen(ctx context.Context, signature string) (bool, error)
	WatchersOf(ctx context.Context, addresses map[string]struct{}) ([]store.Watch, error)
	Symbols(ctx context.Context, mints []string) (map[string]string, error)
	PutSymbols(ctx context.Context, syms map[string]string) error
}

// TokenLookup resolves mint metadata (Helius DAS getAssetBatch).
type TokenLookup interface {
	TokenMetadata(ctx context.Context, mints []string) (map[string]helius.TokenMeta, error)
}

type Processor struct {
	Store    Store
	Tokens   TokenLookup // optional
	Notifier notify.Notifier
	Log      *slog.Logger

	// Breaker and OnHot implement the hot-address guardrail: when a watched
	// address exceeds the breaker's rate, OnHot is called (once) and its
	// alerts are dropped.
	Breaker *Breaker
	OnHot   func(ctx context.Context, address string, eventsPerWindow int)
}

// Run consumes the queue until it is closed, then returns. Closing the
// queue (after the HTTP server has stopped accepting) is how shutdown
// drains in-flight alerts.
func (p *Processor) Run(ctx context.Context, queue <-chan helius.EnhancedTransaction) {
	for tx := range queue {
		p.Process(ctx, &tx)
	}
}

// Process handles one transaction. Errors are logged, never returned: the
// webhook was already acknowledged, so there's nobody to return them to.
func (p *Processor) Process(ctx context.Context, tx *helius.EnhancedTransaction) {
	log := p.Log.With("signature", tx.Signature, "type", tx.Type, "source", tx.Source)
	if tx.Signature == "" {
		log.Warn("skipping transaction without signature")
		return
	}
	if tx.Failed() {
		log.Debug("skipping failed transaction")
		return
	}

	// Idempotency: Helius retries on timeouts and errors, and documents
	// that duplicates can happen. The signature is the natural key.
	// We mark before notifying, so alerts are at-most-once: a crash
	// mid-send loses an alert rather than spamming a user twice.
	fresh, err := p.Store.MarkSeen(ctx, tx.Signature)
	if err != nil {
		log.Error("dedupe check failed", "err", err)
		return
	}
	if !fresh {
		log.Info("duplicate delivery ignored")
		return
	}

	watchers, err := p.Store.WatchersOf(ctx, tx.Accounts())
	if err != nil {
		log.Error("watcher lookup failed", "err", err)
		return
	}
	if len(watchers) == 0 {
		// Happens briefly after /unwatch, before the webhook update lands.
		log.Debug("no watchers for transaction")
		return
	}

	// Count one event per watched address in this tx, before any alerting.
	now := time.Now()
	hot := map[string]bool{}
	for _, w := range watchers {
		if _, done := hot[w.Address]; done {
			continue
		}
		tripped, n := p.Breaker.Observe(w.Address, now)
		hot[w.Address] = tripped
		if tripped {
			log.Warn("hot address breaker tripped", "address", w.Address, "events_per_min", n)
			if p.OnHot != nil {
				p.OnHot(ctx, w.Address, n)
			}
		}
	}

	sym := p.symbols(ctx, alert.Mints(tx), log)
	lag := time.Since(time.Unix(tx.Timestamp, 0)).Round(100 * time.Millisecond)
	for _, w := range watchers {
		if hot[w.Address] {
			continue
		}
		a := alert.Build(tx, w.Address, w.Label, sym)
		if err := p.Notifier.Notify(ctx, w.ChatID, a); err != nil {
			log.Error("notify failed", "chat_id", w.ChatID, "err", err)
			continue
		}
		log.Info("alert routed", "chat_id", w.ChatID, "address", w.Address, "lag", lag)
	}
}

// symbols resolves mints from the SQLite cache first, then DAS for the
// rest. Lookup failures degrade to shortened mint addresses.
func (p *Processor) symbols(ctx context.Context, mints []string, log *slog.Logger) alert.Symbols {
	sym := alert.Symbols{}
	if len(mints) == 0 {
		return sym
	}
	cached, err := p.Store.Symbols(ctx, mints)
	if err != nil {
		log.Warn("symbol cache read failed", "err", err)
	}
	var missing []string
	for _, m := range mints {
		if s, ok := cached[m]; ok {
			sym[m] = s
		} else {
			missing = append(missing, m)
		}
	}
	if len(missing) == 0 || p.Tokens == nil {
		return sym
	}
	lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	meta, err := p.Tokens.TokenMetadata(lctx, missing)
	if err != nil {
		log.Warn("DAS symbol lookup failed", "err", err, "mints", len(missing))
		return sym
	}
	fresh := map[string]string{}
	for m, t := range meta {
		sym[m] = t.Symbol
		fresh[m] = t.Symbol
	}
	if err := p.Store.PutSymbols(ctx, fresh); err != nil {
		log.Warn("symbol cache write failed", "err", err)
	}
	return sym
}
