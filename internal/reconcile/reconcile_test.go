package reconcile

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"testing"

	"github.com/teezzan/solwatch/internal/helius"
)

// fakeAPI behaves like the Helius webhook API as observed while building
// this: zero addresses is a 400, unknown IDs are a 404.
type fakeAPI struct {
	hooks map[string]helius.Webhook
	calls []string
	next  int
}

func (f *fakeAPI) ListWebhooks(context.Context) ([]helius.Webhook, error) {
	f.calls = append(f.calls, "list")
	var out []helius.Webhook
	for _, h := range f.hooks {
		out = append(out, h)
	}
	return out, nil
}

func (f *fakeAPI) CreateWebhook(_ context.Context, w helius.Webhook) (*helius.Webhook, error) {
	f.calls = append(f.calls, "create")
	if len(w.AccountAddresses) == 0 {
		return nil, &helius.APIError{Status: 400, Body: "At least one account address is required"}
	}
	f.next++
	w.WebhookID = string(rune('a' + f.next - 1))
	f.hooks[w.WebhookID] = w
	return &w, nil
}

func (f *fakeAPI) UpdateWebhook(_ context.Context, id string, w helius.Webhook) (*helius.Webhook, error) {
	f.calls = append(f.calls, "update")
	if _, ok := f.hooks[id]; !ok {
		return nil, helius.ErrNotFound
	}
	w.WebhookID = id
	f.hooks[id] = w
	return &w, nil
}

func (f *fakeAPI) DeleteWebhook(_ context.Context, id string) error {
	f.calls = append(f.calls, "delete")
	if _, ok := f.hooks[id]; !ok {
		return helius.ErrNotFound
	}
	delete(f.hooks, id)
	return nil
}

type memState struct {
	addrs map[string]bool
	kv    map[string]string
}

func (m *memState) Addresses(context.Context) ([]string, error) {
	var out []string
	for a := range m.addrs {
		out = append(out, a)
	}
	sort.Strings(out)
	return out, nil
}
func (m *memState) Get(_ context.Context, k string) (string, error) { return m.kv[k], nil }
func (m *memState) Set(_ context.Context, k, v string) error        { m.kv[k] = v; return nil }
func (m *memState) Delete(_ context.Context, k string) error        { delete(m.kv, k); return nil }

func setup() (*Reconciler, *fakeAPI, *memState) {
	api := &fakeAPI{hooks: map[string]helius.Webhook{}}
	st := &memState{addrs: map[string]bool{}, kv: map[string]string{}}
	r := &Reconciler{
		API: api, State: st,
		WebhookURL: "https://example.com/webhooks/helius", AuthHeader: "secret",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return r, api, st
}

func TestSyncLifecycle(t *testing.T) {
	ctx := context.Background()
	r, api, st := setup()

	// Nothing watched: no API calls at all.
	if err := r.Sync(ctx); err != nil || len(api.calls) != 0 {
		t.Fatalf("empty sync: err=%v calls=%v", err, api.calls)
	}

	st.addrs["A"] = true
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// First sync looks for an existing hook to adopt, then creates.
	if !reflect.DeepEqual(api.calls, []string{"list", "create"}) {
		t.Fatalf("calls = %v", api.calls)
	}
	h := api.hooks["a"]
	if h.WebhookType != "enhanced" || h.AuthHeader != "secret" || !reflect.DeepEqual(h.TransactionTypes, []string{"ANY"}) {
		t.Errorf("created hook = %+v", h)
	}

	// Unchanged: no call (every edit costs 100 credits).
	api.calls = nil
	r.Sync(ctx)
	if len(api.calls) != 0 {
		t.Fatalf("no-op sync made calls: %v", api.calls)
	}

	// Changed: one full-body update.
	st.addrs["B"] = true
	r.Sync(ctx)
	if !reflect.DeepEqual(api.calls, []string{"update"}) {
		t.Fatalf("calls = %v", api.calls)
	}
	if got := api.hooks["a"].AccountAddresses; !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Errorf("addresses = %v", got)
	}

	// Everything unwatched: delete, since Helius won't accept zero.
	api.calls = nil
	st.addrs = map[string]bool{}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(api.calls, []string{"delete"}) || len(api.hooks) != 0 || st.kv[keyWebhookID] != "" {
		t.Fatalf("calls=%v hooks=%v kv=%v", api.calls, api.hooks, st.kv)
	}
}

func TestSyncAdoptsExistingWebhook(t *testing.T) {
	r, api, st := setup()
	api.hooks["zzz"] = helius.Webhook{WebhookID: "zzz", WebhookURL: r.WebhookURL}
	st.addrs["A"] = true
	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(api.calls, []string{"list", "update"}) || st.kv[keyWebhookID] != "zzz" {
		t.Fatalf("calls=%v id=%q", api.calls, st.kv[keyWebhookID])
	}
}

func TestSyncRecreatesDeletedWebhook(t *testing.T) {
	r, api, st := setup()
	st.addrs["A"] = true
	st.kv[keyWebhookID] = "gone" // deleted from the dashboard behind our back
	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(api.calls, []string{"update", "create"}) || st.kv[keyWebhookID] != "a" {
		t.Fatalf("calls=%v id=%q", api.calls, st.kv[keyWebhookID])
	}
}

func TestURLChangeTriggersUpdate(t *testing.T) {
	ctx := context.Background()
	r, api, st := setup()
	st.addrs["A"] = true
	r.Sync(ctx)
	api.calls = nil
	// Quick tunnels hand out a new hostname on every restart.
	r.WebhookURL = "https://new-tunnel.example.com/webhooks/helius"
	r.Sync(ctx)
	if !reflect.DeepEqual(api.calls, []string{"update"}) || api.hooks["a"].WebhookURL != r.WebhookURL {
		t.Fatalf("calls=%v hook=%+v", api.calls, api.hooks["a"])
	}
}
