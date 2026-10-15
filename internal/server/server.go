// Package server receives Helius webhook deliveries.
//
// Helius wants a 200 within about a second, otherwise it counts the
// delivery as failed and retries (and eventually disables the webhook). So
// the handler only authenticates, decodes and enqueues; the slow part
// (symbol lookups, Telegram) happens in the pipeline.
package server

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/teezzan/solwatch/internal/helius"
)

const maxBody = 16 << 20 // enhanced payloads are ~10-30 KB per tx; be generous

type WebhookHandler struct {
	// Secret must match the authHeader we registered with Helius. Helius
	// sends it back verbatim in the Authorization header.
	Secret string
	Queue  chan<- helius.EnhancedTransaction
	// CaptureDir, if set, saves every authenticated body to disk. That's
	// how the test fixtures in testdata/ were recorded.
	CaptureDir string
	Log        *slog.Logger
	// OnEvents, if set, is told how many transactions each authenticated
	// delivery carried. Every one of them cost a credit.
	OnEvents func(n int)
}

func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	got := r.Header.Get("Authorization")
	if h.Secret == "" || subtle.ConstantTimeCompare([]byte(got), []byte(h.Secret)) != 1 {
		h.Log.Warn("webhook rejected: bad Authorization header", "remote", r.RemoteAddr)
		// 403 is the one 4xx Helius does not retry, which is what we want
		// for a request that will never succeed.
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	if h.CaptureDir != "" {
		h.capture(body)
	}

	var txs []helius.EnhancedTransaction
	if err := json.Unmarshal(body, &txs); err != nil {
		h.Log.Warn("webhook rejected: bad JSON", "err", err, "bytes", len(body))
		http.Error(w, "expected a JSON array of enhanced transactions", http.StatusBadRequest)
		return
	}

	if h.OnEvents != nil {
		h.OnEvents(len(txs))
	}
	for i, tx := range txs {
		select {
		case h.Queue <- tx:
		default:
			// Backpressure: ask Helius to retry rather than silently
			// dropping. Already-queued items are deduped by signature.
			h.Log.Error("queue full, asking Helius to retry", "dropped", len(txs)-i)
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
	}
	h.Log.Debug("webhook accepted", "txs", len(txs), "bytes", len(body))
	w.WriteHeader(http.StatusOK)
}

func (h *WebhookHandler) capture(body []byte) {
	name := filepath.Join(h.CaptureDir, fmt.Sprintf("%d.json", time.Now().UnixNano()))
	if err := os.WriteFile(name, body, 0o600); err != nil {
		h.Log.Warn("capture failed", "err", err)
	}
}

// Mux wires the webhook handler and a health check.
func Mux(path string, hook http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle(path, hook)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	return mux
}
