package helius

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultAPIBase = "https://mainnet.helius-rpc.com"
	DefaultRPCURL  = "https://mainnet.helius-rpc.com"
)

// Client talks to the Helius webhook management API and the DAS RPC.
//
// The API key travels as an ?api-key= query parameter (the documented way),
// which means it's inside every request URL. net/http puts the URL into
// *url.Error, so every error this client returns is scrubbed of the key.
type Client struct {
	APIKey  string
	APIBase string // webhook management API, default DefaultAPIBase
	RPCURL  string // JSON-RPC endpoint for DAS, default DefaultRPCURL
	HTTP    *http.Client
}

func NewClient(apiKey string) *Client {
	return &Client{
		APIKey:  apiKey,
		APIBase: DefaultAPIBase,
		RPCURL:  DefaultRPCURL,
		HTTP:    &http.Client{Timeout: 15 * time.Second},
	}
}

// ErrNotFound is returned when a webhook ID no longer exists on Helius.
var ErrNotFound = errors.New("helius: not found")

// APIError is a non-2xx response from Helius.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("helius: HTTP %d: %s", e.Status, e.Body)
}

// Webhook mirrors the webhook object the management API accepts and returns.
type Webhook struct {
	WebhookID        string   `json:"webhookID,omitempty"`
	WebhookURL       string   `json:"webhookURL"`
	TransactionTypes []string `json:"transactionTypes"`
	AccountAddresses []string `json:"accountAddresses"`
	WebhookType      string   `json:"webhookType"`
	AuthHeader       string   `json:"authHeader,omitempty"`
	Active           bool     `json:"active,omitempty"`
}

func (c *Client) ListWebhooks(ctx context.Context) ([]Webhook, error) {
	var out []Webhook
	err := c.do(ctx, http.MethodGet, "/v0/webhooks", nil, &out)
	return out, err
}

func (c *Client) GetWebhook(ctx context.Context, id string) (*Webhook, error) {
	var out Webhook
	if err := c.do(ctx, http.MethodGet, "/v0/webhooks/"+url.PathEscape(id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) CreateWebhook(ctx context.Context, w Webhook) (*Webhook, error) {
	var out Webhook
	if err := c.do(ctx, http.MethodPost, "/v0/webhooks", w, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdateWebhook replaces the webhook config. Helius validates PUT bodies like
// POST bodies: webhookURL, webhookType and transactionTypes are required even
// if you only want to change the address list.
func (c *Client) UpdateWebhook(ctx context.Context, id string, w Webhook) (*Webhook, error) {
	w.WebhookID = ""
	var out Webhook
	if err := c.do(ctx, http.MethodPut, "/v0/webhooks/"+url.PathEscape(id), w, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) DeleteWebhook(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v0/webhooks/"+url.PathEscape(id), nil, nil)
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	u := strings.TrimRight(c.APIBase, "/") + path + "?api-key=" + url.QueryEscape(c.APIKey)
	return c.send(ctx, method, u, in, out)
}

func (c *Client) send(ctx context.Context, method, u string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return c.scrub(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return c.scrub(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return c.scrub(err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode/100 != 2 {
		return &APIError{Status: resp.StatusCode, Body: c.redact(string(raw))}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("helius: decode %s response: %w", method, err)
	}
	return nil
}

// scrub flattens an error to a string with the API key removed. We lose
// errors.Is on the wrapped error, but a leaked key in a log line is worse.
func (c *Client) scrub(err error) error {
	if err == nil {
		return nil
	}
	msg := "helius: " + c.redact(err.Error())
	// Keep context errors matchable so shutdown paths can recognise them.
	for _, target := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, target) {
			return fmt.Errorf("%s: %w", msg, target)
		}
	}
	return errors.New(msg)
}

func (c *Client) redact(s string) string {
	if c.APIKey == "" {
		return s
	}
	return strings.ReplaceAll(s, c.APIKey, "REDACTED")
}
