package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teezzan/solwatch/internal/helius"
)

func fixture(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "payloads", "swap_sol_to_token.json"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func newHandler(queue chan helius.EnhancedTransaction) *WebhookHandler {
	return &WebhookHandler{
		Secret: "s3cret",
		Queue:  queue,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func post(h http.Handler, auth, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/webhooks/helius", strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestWebhookAuth(t *testing.T) {
	q := make(chan helius.EnhancedTransaction, 4)
	h := newHandler(q)
	body := fixture(t)

	for _, auth := range []string{"", "wrong", "Bearer s3cret", "s3cret "} {
		if rec := post(h, auth, body); rec.Code != http.StatusForbidden {
			t.Errorf("auth %q: status %d, want 403", auth, rec.Code)
		}
	}
	if len(q) != 0 {
		t.Fatal("unauthenticated request was enqueued")
	}

	rec := post(h, "s3cret", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid request: status %d", rec.Code)
	}
	tx := <-q
	if tx.Type != "SWAP" || tx.Signature == "" {
		t.Errorf("decoded tx = %+v", tx)
	}
}

func TestWebhookRejectsGarbage(t *testing.T) {
	h := newHandler(make(chan helius.EnhancedTransaction, 1))
	if rec := post(h, "s3cret", `{"not":"an array"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("object body: status %d, want 400", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/webhooks/helius", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: status %d, want 405", rec.Code)
	}
}

func TestWebhookBackpressure(t *testing.T) {
	q := make(chan helius.EnhancedTransaction) // unbuffered, nobody reading
	if rec := post(newHandler(q), "s3cret", fixture(t)); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("full queue: status %d, want 503 so Helius retries", rec.Code)
	}
}

func TestCaptureDir(t *testing.T) {
	dir := t.TempDir()
	h := newHandler(make(chan helius.EnhancedTransaction, 1))
	h.CaptureDir = dir
	post(h, "s3cret", fixture(t))
	post(h, "bad", fixture(t)) // must not be captured
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	if len(files) != 1 {
		t.Fatalf("captured %d files, want 1", len(files))
	}
}
