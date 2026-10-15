// Package helius contains the small slice of the Helius API that solwatch
// uses: the Enhanced Transaction payload, webhook management, and a DAS
// lookup for token symbols.
package helius

import "encoding/json"

// EnhancedTransaction is one element of the JSON array Helius POSTs to an
// "enhanced" webhook. Only the fields solwatch reads are modelled; the rest
// (instructions, lighthouseData, ...) are ignored by encoding/json.
type EnhancedTransaction struct {
	Signature        string           `json:"signature"`
	Slot             uint64           `json:"slot"`
	Timestamp        int64            `json:"timestamp"`
	Type             string           `json:"type"`
	Source           string           `json:"source"`
	Description      string           `json:"description"`
	Fee              uint64           `json:"fee"`
	FeePayer         string           `json:"feePayer"`
	NativeTransfers  []NativeTransfer `json:"nativeTransfers"`
	TokenTransfers   []TokenTransfer  `json:"tokenTransfers"`
	AccountData      []AccountData    `json:"accountData"`
	Events           Events           `json:"events"`
	TransactionError json.RawMessage  `json:"transactionError"`
}

type NativeTransfer struct {
	FromUserAccount string `json:"fromUserAccount"`
	ToUserAccount   string `json:"toUserAccount"`
	Amount          uint64 `json:"amount"` // lamports
}

type TokenTransfer struct {
	FromUserAccount  string  `json:"fromUserAccount"`
	ToUserAccount    string  `json:"toUserAccount"`
	FromTokenAccount string  `json:"fromTokenAccount"`
	ToTokenAccount   string  `json:"toTokenAccount"`
	Mint             string  `json:"mint"`
	TokenAmount      float64 `json:"tokenAmount"` // already scaled by decimals
	TokenStandard    string  `json:"tokenStandard"`
}

type AccountData struct {
	Account             string               `json:"account"`
	NativeBalanceChange int64                `json:"nativeBalanceChange"`
	TokenBalanceChanges []TokenBalanceChange `json:"tokenBalanceChanges"`
}

type TokenBalanceChange struct {
	UserAccount    string         `json:"userAccount"`
	TokenAccount   string         `json:"tokenAccount"`
	Mint           string         `json:"mint"`
	RawTokenAmount RawTokenAmount `json:"rawTokenAmount"`
}

// RawTokenAmount is an integer amount (as a string, to survive >2^53) plus
// the mint's decimals.
type RawTokenAmount struct {
	TokenAmount string `json:"tokenAmount"`
	Decimals    int    `json:"decimals"`
}

type Events struct {
	Swap *SwapEvent `json:"swap"`
}

// SwapEvent is Helius' normalised view of a swap, regardless of which DEX or
// aggregator executed it.
type SwapEvent struct {
	NativeInput  *NativeAmount `json:"nativeInput"`
	NativeOutput *NativeAmount `json:"nativeOutput"`
	TokenInputs  []TokenAmount `json:"tokenInputs"`
	TokenOutputs []TokenAmount `json:"tokenOutputs"`
}

type NativeAmount struct {
	Account string `json:"account"`
	Amount  string `json:"amount"` // lamports, as a string
}

type TokenAmount struct {
	UserAccount    string         `json:"userAccount"`
	TokenAccount   string         `json:"tokenAccount"`
	Mint           string         `json:"mint"`
	RawTokenAmount RawTokenAmount `json:"rawTokenAmount"`
}

// Failed reports whether the transaction failed on chain. Enhanced webhooks
// don't deliver failed transactions today, but the field exists, so check it.
func (t *EnhancedTransaction) Failed() bool {
	s := string(t.TransactionError)
	return s != "" && s != "null"
}

// Accounts returns every address the transaction touched, as far as the
// parsed payload tells us: fee payer, account data, and both sides of every
// transfer. Used to route a transaction to the users watching any of them.
func (t *EnhancedTransaction) Accounts() map[string]struct{} {
	set := make(map[string]struct{})
	add := func(a string) {
		if a != "" {
			set[a] = struct{}{}
		}
	}
	add(t.FeePayer)
	for _, a := range t.AccountData {
		add(a.Account)
		for _, c := range a.TokenBalanceChanges {
			add(c.UserAccount)
		}
	}
	for _, n := range t.NativeTransfers {
		add(n.FromUserAccount)
		add(n.ToUserAccount)
	}
	for _, tt := range t.TokenTransfers {
		add(tt.FromUserAccount)
		add(tt.ToUserAccount)
	}
	return set
}
