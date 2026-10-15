// Package reconcile keeps one Helius webhook in sync with the set of
// watched addresses.
//
// solwatch uses a single "enhanced" webhook for every user. Each
// /watch or /unwatch changes the desired address list; the reconciler
// debounces those changes and PUTs the full list to Helius. Every
// create/update/delete costs 100 credits, so it skips no-op updates and
// batches bursts of commands into one call.
package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/teezzan/solwatch/internal/helius"
)

// API is the subset of the Helius client the reconciler needs.
type API interface {
	ListWebhooks(ctx context.Context) ([]helius.Webhook, error)
	CreateWebhook(ctx context.Context, w helius.Webhook) (*helius.Webhook, error)
	UpdateWebhook(ctx context.Context, id string, w helius.Webhook) (*helius.Webhook, error)
	DeleteWebhook(ctx context.Context, id string) error
}

// State is where the reconciler remembers the webhook ID between runs.
type State interface {
	Addresses(ctx context.Context) ([]string, error)
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
}

const (
	keyWebhookID   = "helius_webhook_id"
	keyWebhookHash = "helius_webhook_hash"
)

type Reconciler struct {
	API         API
	State       State
	WebhookURL  string // public URL Helius will POST to
	AuthHeader  string // echoed back by Helius in the Authorization header
	WebhookType string // "enhanced" (mainnet) or "enhancedDevnet"
	Debounce    time.Duration
	Log         *slog.Logger

	kick chan struct{}
}

func (r *Reconciler) init() {
	if r.kick == nil {
		r.kick = make(chan struct{}, 1)
	}
	if r.WebhookType == "" {
		r.WebhookType = "enhanced"
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
}

// Trigger asks for a sync soon. It never blocks.
func (r *Reconciler) Trigger() {
	r.init()
	select {
	case r.kick <- struct{}{}:
	default: // a sync is already pending
	}
}

// Run syncs once at startup, then after every Trigger (debounced), retrying
// failures with backoff, until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) {
	r.init()
	r.Trigger()
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.kick:
		}
		// Debounce: let a burst of /watch commands settle into one API call.
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.Debounce):
		}
		if err := r.Sync(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			r.Log.Error("webhook sync failed", "err", err, "retry_in", backoff)
			time.AfterFunc(backoff, r.Trigger)
			backoff = min(backoff*2, 5*time.Minute)
			continue
		}
		backoff = time.Second
	}
}

// Sync makes Helius match the store. It is safe to call repeatedly.
func (r *Reconciler) Sync(ctx context.Context) error {
	r.init()
	addrs, err := r.State.Addresses(ctx)
	if err != nil {
		return err
	}
	id, err := r.State.Get(ctx, keyWebhookID)
	if err != nil {
		return err
	}

	// Helius rejects a webhook with zero addresses ("At least one account
	// address is required"), so an empty watch list means deleting it.
	if len(addrs) == 0 {
		if id == "" {
			return nil
		}
		return r.Teardown(ctx)
	}

	want := helius.Webhook{
		WebhookURL:       r.WebhookURL,
		TransactionTypes: []string{"ANY"},
		AccountAddresses: addrs,
		WebhookType:      r.WebhookType,
		AuthHeader:       r.AuthHeader,
	}
	hash := r.hash(want)

	if id == "" {
		id, err = r.adopt(ctx)
		if err != nil {
			return err
		}
	} else if last, _ := r.State.Get(ctx, keyWebhookHash); last == hash {
		r.Log.Debug("webhook already in sync", "webhook_id", id, "addresses", len(addrs))
		return nil
	}

	if id != "" {
		_, err = r.API.UpdateWebhook(ctx, id, want)
		if errors.Is(err, helius.ErrNotFound) {
			r.Log.Warn("webhook vanished on Helius, recreating", "webhook_id", id)
			id = ""
		} else if err != nil {
			return err
		} else {
			r.Log.Info("webhook updated", "webhook_id", id, "addresses", len(addrs))
		}
	}
	if id == "" {
		created, err := r.API.CreateWebhook(ctx, want)
		if err != nil {
			return err
		}
		id = created.WebhookID
		r.Log.Info("webhook created", "webhook_id", id, "addresses", len(addrs))
	}
	if err := r.State.Set(ctx, keyWebhookID, id); err != nil {
		return err
	}
	return r.State.Set(ctx, keyWebhookHash, hash)
}

// Teardown deletes the webhook, if any. Used when the last address is
// unwatched and, optionally, on shutdown.
func (r *Reconciler) Teardown(ctx context.Context) error {
	r.init()
	id, err := r.State.Get(ctx, keyWebhookID)
	if err != nil || id == "" {
		return err
	}
	if err := r.API.DeleteWebhook(ctx, id); err != nil && !errors.Is(err, helius.ErrNotFound) {
		return err
	}
	r.Log.Info("webhook deleted", "webhook_id", id)
	_ = r.State.Delete(ctx, keyWebhookHash)
	return r.State.Delete(ctx, keyWebhookID)
}

// adopt finds an existing webhook pointing at our URL, e.g. after the
// database was wiped, so we don't leak webhooks (plans cap how many you
// can have).
func (r *Reconciler) adopt(ctx context.Context) (string, error) {
	hooks, err := r.API.ListWebhooks(ctx)
	if err != nil {
		return "", err
	}
	for _, h := range hooks {
		if h.WebhookURL == r.WebhookURL {
			r.Log.Info("adopting existing webhook", "webhook_id", h.WebhookID)
			return h.WebhookID, nil
		}
	}
	return "", nil
}

func (r *Reconciler) hash(w helius.Webhook) string {
	h := sha256.New()
	h.Write([]byte(w.WebhookURL + "\n" + w.WebhookType + "\n" + w.AuthHeader + "\n"))
	h.Write([]byte(strings.Join(w.AccountAddresses, ",")))
	return hex.EncodeToString(h.Sum(nil))
}
