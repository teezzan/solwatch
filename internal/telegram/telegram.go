// Package telegram is a minimal Telegram Bot API client: long-polling for
// messages and sending replies. Two endpoints don't justify a dependency.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	Token string
	Base  string // default https://api.telegram.org
	HTTP  *http.Client
}

func New(token string) *Client {
	return &Client{
		Token: token,
		Base:  "https://api.telegram.org",
		// Long polls hold the connection for up to 30s; give them headroom.
		HTTP: &http.Client{Timeout: 45 * time.Second},
	}
}

type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message"`
}

type Message struct {
	MessageID int64  `json:"message_id"`
	Text      string `json:"text"`
	Chat      struct {
		ID int64 `json:"id"`
	} `json:"chat"`
}

// RetryAfterError is Telegram's flood control response (HTTP 429).
type RetryAfterError struct{ After time.Duration }

func (e *RetryAfterError) Error() string {
	return fmt.Sprintf("telegram: rate limited, retry after %s", e.After)
}

// GetUpdates long-polls for new messages after offset.
func (c *Client) GetUpdates(ctx context.Context, offset int64, timeout time.Duration) ([]Update, error) {
	var out []Update
	err := c.call(ctx, "getUpdates", map[string]any{
		"offset":          offset,
		"timeout":         int(timeout.Seconds()),
		"allowed_updates": []string{"message"},
	}, &out)
	return out, err
}

// SendMessage sends HTML-formatted text. On a 429 it waits once for the
// advertised retry_after and tries again.
func (c *Client) SendMessage(ctx context.Context, chatID int64, html string) error {
	body := map[string]any{
		"chat_id":                  chatID,
		"text":                     html,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	err := c.call(ctx, "sendMessage", body, nil)
	var ra *RetryAfterError
	if errors.As(err, &ra) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ra.After):
		}
		err = c.call(ctx, "sendMessage", body, nil)
	}
	return err
}

func (c *Client) call(ctx context.Context, method string, in, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	url := strings.TrimRight(c.Base, "/") + "/bot" + c.Token + "/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return c.scrub(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return c.scrub(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return c.scrub(err)
	}
	var env struct {
		OK          bool            `json:"ok"`
		Result      json.RawMessage `json:"result"`
		Description string          `json:"description"`
		Parameters  struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("telegram %s: HTTP %d: undecodable body", method, resp.StatusCode)
	}
	if !env.OK {
		if resp.StatusCode == http.StatusTooManyRequests && env.Parameters.RetryAfter > 0 {
			return &RetryAfterError{After: time.Duration(env.Parameters.RetryAfter) * time.Second}
		}
		return fmt.Errorf("telegram %s: %s", method, env.Description)
	}
	if out != nil {
		return json.Unmarshal(env.Result, out)
	}
	return nil
}

// scrub removes the bot token, which lives in the URL path, from errors.
func (c *Client) scrub(err error) error {
	msg := strings.ReplaceAll(err.Error(), c.Token, "REDACTED")
	for _, target := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, target) {
			return fmt.Errorf("telegram: %s: %w", msg, target)
		}
	}
	return errors.New("telegram: " + msg)
}
