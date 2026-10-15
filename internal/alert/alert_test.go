package alert

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/teezzan/solwatch/internal/helius"
)

// Wallets that appear in the captured fixtures. They're busy mainnet
// traders picked from recent Jupiter activity, not anyone I know.
const (
	trader1 = "2KNuy5aBQB58YeLRtj3pJWyXHztaf2jqHWnowThanHmZ"
	trader2 = "HhgBQmx2Ja7hRRxVA53xVxY9jaWTt3GsNiVKcHHYn51F"
	trader3 = "GHnWxdj8qg2trFLHD5YbjAARVzahbjCghjLiFyMcsoBH"
	trader4 = "7crqCyp2x65GJPee3yCovrx2NgDVByY3YJzxRMoieuGf"
)

// loadFixture reads a real webhook body captured with CAPTURE_DIR. Every
// delivery we captured carried exactly one transaction.
func loadFixture(t *testing.T, name string) *helius.EnhancedTransaction {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "payloads", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var txs []helius.EnhancedTransaction
	if err := json.Unmarshal(b, &txs); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(txs) != 1 {
		t.Fatalf("%s: want 1 tx, got %d", name, len(txs))
	}
	return &txs[0]
}

func TestSummarizeFixtures(t *testing.T) {
	tests := []struct {
		fixture string
		watched string
		want    string
	}{
		// Helius classified it and filled events.swap: the easy path.
		{"swap_sol_to_token", trader1, "swapped 0.08221 SOL → 828,678.81 G6kN…pump on Jupiter"},
		{"swap_token_to_sol", trader1, "swapped 1,212,244.68 7RqQ…pump → 0.1294 SOL on Jupiter"},
		// type SWAP, empty description, but events.swap still works.
		{"swap_no_description", trader2, "swapped 17.4661 SOL → 14,145,540.93 HBmj…bonk on Jupiter"},
		// A SWAP someone else signed that paid our wallet: not "our" swap.
		{"swap_third_party_pays_watched", trader2, "received 16.2617 SOL from 7Qjx…PxEi"},
		// Helius couldn't classify these, but balance flows tell the story.
		{"unknown_dflow_route", trader1, "swapped 0.08358 SOL → 782,307.64 G6kN…pump + 0.000392 SPCX…imGb on DFlow"},
		{"close_account_sell", trader2, "swapped 14,145,540.93 HBmj…bonk → 2.8515 SOL"},
		{"initialize_account_buy", trader3, "swapped 0.1013 SOL → 50,928.19 4uWm…pump"},
		{"unknown_pump_fun", trader3, "received 0.1398 33jE…pump from D17v…qDad"},
		// Transfers.
		{"transfer_sol", trader3, "sent 0.0005252 SOL to gasR…6hPT"},
		{"transfer_multiple", trader3, "received 0.0233 SOL"},
		{"transfer_token_and_sol", trader4, "sent 0.002 SOL to HLnp…TLcC, received 13,834.72 4NMx…gMD5 from HLnp…TLcC"},
		// Token-2022 account close: UNKNOWN, no description, no transfers.
		// Only accountData shows the rent refund.
		{"unknown_no_flows", trader4, "transaction: SOL balance +0.001514"},
	}
	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			tx := loadFixture(t, tt.fixture)
			if _, ok := tx.Accounts()[tt.watched]; !ok {
				t.Fatalf("fixture does not involve %s", tt.watched)
			}
			if got := Summarize(tx, tt.watched, nil); got != tt.want {
				t.Errorf("Summarize()\n got: %q\nwant: %q", got, tt.want)
			}
		})
	}
}

func TestSummarizeUsesResolvedSymbols(t *testing.T) {
	tx := loadFixture(t, "swap_sol_to_token")
	sym := Symbols{"G6kNgzAurBQqppvRYkEhTLUJ71JhwCsiJnrwHjqnpump": "GROK"}
	got := Summarize(tx, trader1, sym)
	if want := "swapped 0.08221 SOL → 828,678.81 GROK on Jupiter"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestMintsSkipsWellKnown(t *testing.T) {
	tx := loadFixture(t, "swap_sol_to_token")
	got := Mints(tx)
	want := []string{"G6kNgzAurBQqppvRYkEhTLUJ71JhwCsiJnrwHjqnpump", "SPCXxcqXj6e5dJDVNovHN8744zkbhM2bYudU45BimGb"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Mints() = %v, want %v (USDC and wSOL must not trigger a DAS lookup)", got, want)
	}
}

func TestRewriteDescription(t *testing.T) {
	// The NFT sale example from the Helius webhook docs: a typed event
	// where Helius' own sentence beats anything we'd compute.
	desc := "5DxD5ViWjvRZEkxQEaJHZw2sBsso6xoXx3wGFNKgXUzE sold Fox #7637 to CKs1E69a2e9TmH4mKKLrXFF8kD3ZnwKjoEuXa6sz9WqX for 72 SOL on MAGIC_EDEN."
	got := rewriteDescription(desc, "5DxD5ViWjvRZEkxQEaJHZw2sBsso6xoXx3wGFNKgXUzE", nil)
	if want := "sold Fox #7637 to CKs1…9WqX for 72 SOL on MAGIC_EDEN"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	// Watched as the object, plus a mint we know and a long decimal.
	desc = "AAAAhaR1BKzXHzTo9TPYx5ZkxJDXjMeVKqB8bYnS5WGU swapped 0.082209413 SOL for 1000.5 EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v with 7crqCyp2x65GJPee3yCovrx2NgDVByY3YJzxRMoieuGf"
	got = rewriteDescription(desc, trader4, nil)
	if want := "AAAA…5WGU swapped 0.08221 SOL for 1,000.5 USDC with this wallet"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAmount(t *testing.T) {
	tests := map[float64]string{
		0:            "0",
		0.000000001:  "0.000000001",
		0.0005252050: "0.0005252",
		0.082209413:  "0.08221",
		1:            "1",
		2.5:          "2.5",
		17.46612345:  "17.4661",
		999.99999:    "1000",
		1000:         "1,000",
		828678.8133:  "828,678.81",
		14145540.931: "14,145,540.93",
	}
	for in, want := range tests {
		if got := Amount(in); got != want {
			t.Errorf("Amount(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestHTMLEscapesLabel(t *testing.T) {
	a := Alert{Signature: "sig", Label: "<b>me</b> & co", Summary: "sent 1 SOL to x"}
	got := a.HTML("https://solscan.io/tx/%s")
	if strings.Contains(got, "<b>me</b>") || !strings.Contains(got, "&lt;b&gt;me&lt;/b&gt; &amp; co") {
		t.Errorf("label not escaped: %s", got)
	}
	if !strings.Contains(got, `<a href="https://solscan.io/tx/sig">view tx</a>`) {
		t.Errorf("missing link: %s", got)
	}
}

func TestBuildDefaultsLabelToShortAddress(t *testing.T) {
	tx := loadFixture(t, "transfer_sol")
	a := Build(tx, trader3, "", nil)
	if a.Label != "GHnW…soBH" {
		t.Errorf("label = %q", a.Label)
	}
	if got := a.Text(""); !strings.HasPrefix(got, "🔔 GHnW…soBH: sent ") || !strings.HasSuffix(got, "https://solscan.io/tx/"+tx.Signature) {
		t.Errorf("Text() = %q", got)
	}
}
