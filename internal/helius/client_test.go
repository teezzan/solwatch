package helius

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testKey = "test-key-123"

func TestClientNeverLeaksKeyInErrors(t *testing.T) {
	c := NewClient(testKey)
	c.APIBase = "http://127.0.0.1:1" // nothing listens here
	_, err := c.ListWebhooks(context.Background())
	if err == nil {
		t.Fatal("expected a connection error")
	}
	if strings.Contains(err.Error(), testKey) {
		t.Fatalf("API key leaked into error: %v", err)
	}
}

func TestUpdateSendsFullBodyAndMaps404(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("api-key") != testKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/v0/webhooks/missing" {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"error":"not found"}`)
			return
		}
		json.NewDecoder(r.Body).Decode(&gotBody)
		io.WriteString(w, `{"webhookID":"abc","webhookURL":"https://x/y","accountAddresses":["A"],"transactionTypes":["ANY"],"webhookType":"enhanced"}`)
	}))
	defer srv.Close()

	c := NewClient(testKey)
	c.APIBase = srv.URL
	w := Webhook{WebhookID: "ignored", WebhookURL: "https://x/y", TransactionTypes: []string{"ANY"}, AccountAddresses: []string{"A"}, WebhookType: "enhanced", AuthHeader: "s"}
	got, err := c.UpdateWebhook(context.Background(), "abc", w)
	if err != nil {
		t.Fatal(err)
	}
	if got.WebhookID != "abc" {
		t.Errorf("WebhookID = %q", got.WebhookID)
	}
	// Helius rejects PUTs missing webhookURL / webhookType / transactionTypes.
	for _, k := range []string{"webhookURL", "webhookType", "transactionTypes", "accountAddresses", "authHeader"} {
		if _, ok := gotBody[k]; !ok {
			t.Errorf("PUT body missing %q: %v", k, gotBody)
		}
	}
	if _, ok := gotBody["webhookID"]; ok {
		t.Errorf("PUT body should not carry webhookID: %v", gotBody)
	}

	_, err = c.UpdateWebhook(context.Background(), "missing", w)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestFailed(t *testing.T) {
	var ok, bad EnhancedTransaction
	json.Unmarshal([]byte(`{"transactionError":null}`), &ok)
	json.Unmarshal([]byte(`{"transactionError":{"error":"0x1"}}`), &bad)
	if ok.Failed() || !bad.Failed() {
		t.Errorf("Failed(): ok=%v bad=%v", ok.Failed(), bad.Failed())
	}
}
