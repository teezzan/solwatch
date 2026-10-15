// Package alert turns a Helius Enhanced Transaction into a one-line,
// human-readable alert from the point of view of one watched address.
//
// Everything here is pure: no I/O, no clocks. That's what makes it easy to
// test against payloads captured from real webhook deliveries.
package alert

import (
	"fmt"
	"html"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/teezzan/solwatch/internal/helius"
)

const (
	// WrappedSOL is the SPL mint for wrapped SOL. Swaps routinely wrap and
	// unwrap, so we fold it into native SOL for display.
	WrappedSOL = "So11111111111111111111111111111111111111112"
	solKey     = "SOL"
	lamports   = 1e9
)

// Symbols maps a mint address to a ticker, e.g. from DAS getAssetBatch.
type Symbols map[string]string

// wellKnown saves a DAS round trip for the mints that show up in nearly
// every swap.
var wellKnown = Symbols{
	WrappedSOL: "SOL",
	"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
	"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
}

// Alert is a formatted notification ready to hand to a Notifier.
type Alert struct {
	Signature string
	Address   string
	Label     string
	Summary   string // plain text, e.g. "swapped 2 SOL → 310 USDC on Jupiter"
}

// Text renders the alert as plain text.
func (a Alert) Text(explorer string) string {
	return fmt.Sprintf("🔔 %s: %s · %s", a.Label, a.Summary, txURL(explorer, a.Signature))
}

// HTML renders the alert for Telegram's HTML parse mode. Label and summary
// are user/chain controlled, so both are escaped.
func (a Alert) HTML(explorer string) string {
	return fmt.Sprintf(`🔔 <b>%s</b>: %s · <a href="%s">view tx</a>`,
		html.EscapeString(a.Label), html.EscapeString(a.Summary),
		html.EscapeString(txURL(explorer, a.Signature)))
}

func txURL(explorer, sig string) string {
	if explorer == "" {
		explorer = "https://solscan.io/tx/%s"
	}
	return fmt.Sprintf(explorer, sig)
}

// Build creates the alert for one watched address. label may be empty, in
// which case the shortened address is used.
func Build(tx *helius.EnhancedTransaction, watched, label string, sym Symbols) Alert {
	if label == "" {
		label = Short(watched)
	}
	return Alert{
		Signature: tx.Signature,
		Address:   watched,
		Label:     label,
		Summary:   Summarize(tx, watched, sym),
	}
}

// Mints lists every token mint referenced by tx, so the caller can resolve
// symbols in one batch before formatting.
func Mints(tx *helius.EnhancedTransaction) []string {
	set := map[string]struct{}{}
	for _, t := range tx.TokenTransfers {
		set[t.Mint] = struct{}{}
	}
	if s := tx.Events.Swap; s != nil {
		for _, t := range s.TokenInputs {
			set[t.Mint] = struct{}{}
		}
		for _, t := range s.TokenOutputs {
			set[t.Mint] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for m := range set {
		if _, ok := wellKnown[m]; !ok && m != "" {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out
}

// Summarize describes tx from watched's perspective, in order of preference:
//  1. Helius' structured swap event, if watched is the swapper.
//  2. For SWAP and TRANSFER types, watched's net balance flows.
//  3. Helius' own description, with addresses shortened and mints named
//     (best for NFT sales, staking and other typed events).
//  4. Net flows for anything else Helius couldn't describe; in practice a
//     lot of UNKNOWN / CLOSE_ACCOUNT transactions are swaps through routers
//     Helius doesn't parse.
//  5. The bare transaction type.
func Summarize(tx *helius.EnhancedTransaction, watched string, sym Symbols) string {
	venue := venueSuffix(tx.Source)

	if s := tx.Events.Swap; s != nil {
		if in, out := swapLegs(s, watched, sym); in != "" && out != "" {
			return "swapped " + in + " → " + out + venue
		}
	}

	f := computeFlows(tx, watched)
	sent, recv := f.split(sym)
	hasFlows := len(sent) > 0 || len(recv) > 0

	if hasFlows && (tx.Type == "SWAP" || tx.Type == "TRANSFER") {
		return describeFlows(tx.Type, f, sent, recv, venue)
	}
	if d := rewriteDescription(tx.Description, watched, sym); d != "" {
		return d
	}
	if hasFlows {
		return describeFlows(tx.Type, f, sent, recv, venue)
	}
	// Last resort: SOL that moved without a transfer record, e.g. rent
	// refunded when a token account is closed.
	if d := nativeDelta(tx, watched); math.Abs(d) >= 1e-9 {
		sign := "+"
		if d < 0 {
			sign = "-"
		}
		return humanType(tx.Type) + venue + ": SOL balance " + sign + Amount(math.Abs(d))
	}
	return humanType(tx.Type) + venue
}

// nativeDelta is watched's SOL balance change, excluding the network fee if
// watched paid it (nobody wants an alert for "-0.000005 SOL").
func nativeDelta(tx *helius.EnhancedTransaction, watched string) float64 {
	for _, a := range tx.AccountData {
		if a.Account != watched {
			continue
		}
		d := a.NativeBalanceChange
		if tx.FeePayer == watched {
			d += int64(tx.Fee)
		}
		return float64(d) / lamports
	}
	return 0
}

func describeFlows(txType string, f flows, sent, recv []string, venue string) string {
	if len(sent) > 0 && len(recv) > 0 && txType != "TRANSFER" {
		return "swapped " + strings.Join(sent, " + ") + " → " + strings.Join(recv, " + ") + venue
	}
	var parts []string
	if len(sent) > 0 {
		parts = append(parts, "sent "+strings.Join(sent, " + ")+counterparty(" to ", f.sentTo))
	}
	if len(recv) > 0 {
		parts = append(parts, "received "+strings.Join(recv, " + ")+counterparty(" from ", f.recvFrom))
	}
	return strings.Join(parts, ", ")
}

func swapLegs(s *helius.SwapEvent, watched string, sym Symbols) (in, out string) {
	var ins, outs []string
	if s.NativeInput != nil && s.NativeInput.Account == watched {
		ins = append(ins, Amount(rawToFloat(s.NativeInput.Amount, 9))+" SOL")
	}
	for _, t := range s.TokenInputs {
		if t.UserAccount == watched {
			ins = append(ins, Amount(rawToFloat(t.RawTokenAmount.TokenAmount, t.RawTokenAmount.Decimals))+" "+Symbol(t.Mint, sym))
		}
	}
	if s.NativeOutput != nil && s.NativeOutput.Account == watched {
		outs = append(outs, Amount(rawToFloat(s.NativeOutput.Amount, 9))+" SOL")
	}
	for _, t := range s.TokenOutputs {
		if t.UserAccount == watched {
			outs = append(outs, Amount(rawToFloat(t.RawTokenAmount.TokenAmount, t.RawTokenAmount.Decimals))+" "+Symbol(t.Mint, sym))
		}
	}
	return strings.Join(ins, " + "), strings.Join(outs, " + ")
}

type flows struct {
	net      map[string]float64 // asset key (mint or "SOL") -> signed amount
	sentTo   map[string]struct{}
	recvFrom map[string]struct{}
}

// computeFlows nets out what watched sent and received. Native transfers
// between watched and its own token accounts (wrapping SOL, paying token
// account rent) are skipped: they're bookkeeping, not value moving, and
// counting them would double count every wrapped-SOL swap.
func computeFlows(tx *helius.EnhancedTransaction, watched string) flows {
	f := flows{net: map[string]float64{}, sentTo: map[string]struct{}{}, recvFrom: map[string]struct{}{}}

	own := map[string]bool{}
	for _, t := range tx.TokenTransfers {
		if t.FromUserAccount == watched && t.FromTokenAccount != "" {
			own[t.FromTokenAccount] = true
		}
		if t.ToUserAccount == watched && t.ToTokenAccount != "" {
			own[t.ToTokenAccount] = true
		}
	}
	for _, a := range tx.AccountData {
		for _, c := range a.TokenBalanceChanges {
			if c.UserAccount == watched {
				own[c.TokenAccount] = true
			}
		}
	}

	for _, n := range tx.NativeTransfers {
		amt := float64(n.Amount) / lamports
		switch {
		case n.FromUserAccount == watched && n.ToUserAccount == watched:
		case n.FromUserAccount == watched:
			if own[n.ToUserAccount] {
				continue
			}
			f.net[solKey] -= amt
			f.sentTo[n.ToUserAccount] = struct{}{}
		case n.ToUserAccount == watched:
			if own[n.FromUserAccount] {
				continue
			}
			f.net[solKey] += amt
			f.recvFrom[n.FromUserAccount] = struct{}{}
		}
	}
	for _, t := range tx.TokenTransfers {
		key := t.Mint
		if key == WrappedSOL {
			key = solKey
		}
		switch {
		case t.FromUserAccount == watched && t.ToUserAccount == watched:
		case t.FromUserAccount == watched:
			f.net[key] -= t.TokenAmount
			f.sentTo[t.ToUserAccount] = struct{}{}
		case t.ToUserAccount == watched:
			f.net[key] += t.TokenAmount
			f.recvFrom[t.FromUserAccount] = struct{}{}
		}
	}
	return f
}

// split returns formatted outgoing and incoming amounts, largest first by
// key for stable output. Dust below display precision is dropped.
func (f flows) split(sym Symbols) (sent, recv []string) {
	keys := make([]string, 0, len(f.net))
	for k := range f.net {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := f.net[k]
		if math.Abs(v) < 1e-9 {
			continue
		}
		name := solKey
		if k != solKey {
			name = Symbol(k, sym)
		}
		s := Amount(math.Abs(v)) + " " + name
		if v < 0 {
			sent = append(sent, s)
		} else {
			recv = append(recv, s)
		}
	}
	return sent, recv
}

func counterparty(prep string, set map[string]struct{}) string {
	if len(set) != 1 {
		return ""
	}
	for a := range set {
		if a != "" {
			return prep + Short(a)
		}
	}
	return ""
}

var (
	base58Re  = regexp.MustCompile(`\b[1-9A-HJ-NP-Za-km-z]{32,44}\b`)
	decimalRe = regexp.MustCompile(`\b\d+\.\d+\b`)
)

// rewriteDescription makes Helius' description fit in a chat bubble: the
// watched address (usually the sentence subject) is dropped because the
// label already says who it is, mints become symbols, other addresses are
// shortened, and long decimals are trimmed.
func rewriteDescription(desc, watched string, sym Symbols) string {
	desc = strings.TrimSpace(desc)
	if desc == "" {
		return ""
	}
	desc = strings.TrimSpace(strings.TrimPrefix(desc, watched))
	desc = base58Re.ReplaceAllStringFunc(desc, func(a string) string {
		if a == watched {
			return "this wallet"
		}
		if s, ok := lookup(a, sym); ok {
			return s
		}
		return Short(a)
	})
	desc = decimalRe.ReplaceAllStringFunc(desc, func(n string) string {
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return n
		}
		return Amount(f)
	})
	return strings.TrimSuffix(desc, ".")
}

func lookup(mint string, sym Symbols) (string, bool) {
	if s, ok := wellKnown[mint]; ok {
		return s, true
	}
	if s, ok := sym[mint]; ok && s != "" {
		return s, true
	}
	return "", false
}

// Symbol returns a ticker for mint, or a shortened mint address.
func Symbol(mint string, sym Symbols) string {
	if s, ok := lookup(mint, sym); ok {
		return s
	}
	return Short(mint)
}

// Short abbreviates a base58 address to "AbCd…WxYz".
func Short(addr string) string {
	if len(addr) <= 10 {
		return addr
	}
	return addr[:4] + "…" + addr[len(addr)-4:]
}

func rawToFloat(raw string, decimals int) float64 {
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return v / math.Pow10(decimals)
}

// Amount formats a token amount for humans: thousands separators and two
// decimals for big numbers, four significant digits for small ones.
func Amount(v float64) string {
	a := math.Abs(v)
	var s string
	switch {
	case a >= 1000:
		s = commas(strconv.FormatFloat(v, 'f', 2, 64))
	case a >= 1:
		s = strconv.FormatFloat(v, 'f', 4, 64)
	case a == 0:
		return "0"
	default:
		// 4 significant digits: 0.082209413 -> 0.08221
		digits := 3 - int(math.Floor(math.Log10(a)))
		s = strconv.FormatFloat(v, 'f', digits, 64)
	}
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

func commas(s string) string {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	intPart, frac, _ := strings.Cut(s, ".")
	var b strings.Builder
	for i, r := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	out := b.String()
	if frac != "" {
		out += "." + frac
	}
	if neg {
		out = "-" + out
	}
	return out
}

var venues = map[string]string{
	"JUPITER":        "Jupiter",
	"RAYDIUM":        "Raydium",
	"ORCA":           "Orca",
	"METEORA":        "Meteora",
	"PUMP_FUN":       "Pump.fun",
	"PUMP_AMM":       "PumpSwap",
	"DFLOW":          "DFlow",
	"PHOENIX":        "Phoenix",
	"OPENBOOK":       "OpenBook",
	"MAGIC_EDEN":     "Magic Eden",
	"TENSOR":         "Tensor",
	"OKX_DEX_ROUTER": "OKX DEX",
}

// venueSuffix returns " on Jupiter" etc. Plumbing programs (System, SPL
// Token, ATA) aren't venues, so they produce nothing.
func venueSuffix(source string) string {
	switch source {
	case "", "UNKNOWN", "SYSTEM_PROGRAM", "SOLANA_PROGRAM_LIBRARY", "ASSOCIATED_TOKEN_PROGRAM", "COMPUTE_BUDGET":
		return ""
	}
	if v, ok := venues[source]; ok {
		return " on " + v
	}
	return " on " + titleCase(source)
}

func humanType(t string) string {
	if t == "" || t == "UNKNOWN" {
		return "transaction"
	}
	return strings.ToLower(strings.ReplaceAll(t, "_", " "))
}

func titleCase(s string) string {
	words := strings.Split(strings.ToLower(s), "_")
	for i, w := range words {
		if w != "" {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, " ")
}
