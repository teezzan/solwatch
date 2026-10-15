package helius

import (
	"context"
	"net/url"
	"strings"
)

// TokenMeta is what solwatch needs from a DAS asset to print an amount.
type TokenMeta struct {
	Mint     string
	Symbol   string
	Decimals int
}

// TokenMetadata resolves mints to symbols with one DAS getAssetBatch call.
// Mints DAS doesn't know (or that have no symbol) are simply absent from the
// result; callers fall back to a shortened mint address.
func (c *Client) TokenMetadata(ctx context.Context, mints []string) (map[string]TokenMeta, error) {
	out := make(map[string]TokenMeta, len(mints))
	if len(mints) == 0 {
		return out, nil
	}
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      "solwatch",
		"method":  "getAssetBatch",
		"params":  map[string]any{"ids": mints},
	}
	var resp struct {
		Result []*struct {
			ID      string `json:"id"`
			Content struct {
				Metadata struct {
					Symbol string `json:"symbol"`
				} `json:"metadata"`
			} `json:"content"`
			TokenInfo *struct {
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			} `json:"token_info"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	c.count("das")
	if err := c.send(ctx, "POST", c.rpcURL(), req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, &APIError{Status: 200, Body: c.redact(resp.Error.Message)}
	}
	for _, a := range resp.Result {
		if a == nil {
			continue // DAS returns null for unknown ids
		}
		m := TokenMeta{Mint: a.ID, Symbol: strings.TrimSpace(a.Content.Metadata.Symbol)}
		if a.TokenInfo != nil {
			if s := strings.TrimSpace(a.TokenInfo.Symbol); s != "" {
				m.Symbol = s
			}
			m.Decimals = a.TokenInfo.Decimals
		}
		if m.Symbol != "" {
			out[a.ID] = m
		}
	}
	return out, nil
}

func (c *Client) rpcURL() string {
	return strings.TrimRight(c.RPCURL, "/") + "/?api-key=" + url.QueryEscape(c.APIKey)
}

// AccountInfo is the part of getAccountInfo solwatch needs to decide whether
// an address is a wallet.
type AccountInfo struct {
	Exists     bool
	Executable bool
	Owner      string
}

// GetAccountInfo fetches an account's owner and executable flag without
// its data (dataSlice length 0), so it's one cheap RPC call.
func (c *Client) GetAccountInfo(ctx context.Context, address string) (*AccountInfo, error) {
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      "solwatch",
		"method":  "getAccountInfo",
		"params": []any{address, map[string]any{
			"encoding":   "base64",
			"dataSlice":  map[string]int{"offset": 0, "length": 0},
			"commitment": "confirmed",
		}},
	}
	var resp struct {
		Result *struct {
			Value *struct {
				Executable bool   `json:"executable"`
				Owner      string `json:"owner"`
			} `json:"value"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	c.count("rpc")
	if err := c.send(ctx, "POST", c.rpcURL(), req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, &APIError{Status: 200, Body: c.redact(resp.Error.Message)}
	}
	if resp.Result == nil || resp.Result.Value == nil {
		return &AccountInfo{}, nil // never funded: a valid, empty wallet
	}
	return &AccountInfo{Exists: true, Executable: resp.Result.Value.Executable, Owner: resp.Result.Value.Owner}, nil
}
