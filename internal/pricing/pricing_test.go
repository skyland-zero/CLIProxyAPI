package pricing

import (
	"context"
	"testing"
	"time"

	usage "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

func TestParseOpenAIPricesTextStandardContexts(t *testing.T) {
	text := `Standard
Short context
Long context
Model
Input
Cached input
Output
Input
Cached input
Output
gpt-5.5
$5.00
$0.50
$30.00
$10.00
$1.00
$45.00
gpt-5.4-mini
$0.75
$0.075
$4.50
-
-
-`

	prices := ParseOpenAIPricesText(text, OpenAIPricingURL)
	if len(prices) != 3 {
		t.Fatalf("prices = %d, want 3: %+v", len(prices), prices)
	}
	short, ok := findTestPrice(prices, "gpt-5.5", "short_context")
	if !ok {
		t.Fatalf("missing gpt-5.5 short context price: %+v", prices)
	}
	if short.InputPer1M != 5 || short.CachedInputPer1M != 0.5 || short.OutputPer1M != 30 {
		t.Fatalf("short price = %+v", short)
	}
	long, ok := findTestPrice(prices, "gpt-5.5", "long_context")
	if !ok || long.InputPer1M != 10 || long.OutputPer1M != 45 {
		t.Fatalf("long price = %+v ok=%v", long, ok)
	}
	if _, ok = findTestPrice(prices, "gpt-5.4-mini", "long_context"); ok {
		t.Fatalf("unexpected long-context price for gpt-5.4-mini")
	}
}

func TestEstimateEventCostUsesHashedOpenAIPrice(t *testing.T) {
	previous := DefaultStore()
	store := NewMemoryStore()
	SetDefaultStore(store)
	t.Cleanup(func() { SetDefaultStore(previous) })

	if err := store.UpsertPrices(context.Background(), []ModelPrice{{
		Provider:         ProviderOpenAI,
		Model:            "gpt-5.5",
		Category:         "standard",
		Context:          "short_context",
		Modality:         "text",
		Unit:             "1m_tokens",
		InputPer1M:       5,
		CachedInputPer1M: 0.5,
		OutputPer1M:      30,
		FetchedAt:        time.Now(),
	}}); err != nil {
		t.Fatalf("upsert price: %v", err)
	}

	cost, err := EstimateEventCost(context.Background(), usage.Event{
		Provider: "codex",
		Model:    "gpt-5.5",
		Tokens: usage.TokenStats{
			InputTokens:     1_000_000,
			CachedTokens:    1_000_000,
			OutputTokens:    1_000_000,
			ReasoningTokens: 1_000_000,
		},
	})
	if err != nil {
		t.Fatalf("EstimateEventCost error: %v", err)
	}
	if cost == nil {
		t.Fatalf("cost is nil, want estimate")
	}
	if *cost != 65.5 {
		t.Fatalf("cost = %v, want 65.5", *cost)
	}
}

func findTestPrice(prices []ModelPrice, model, contextName string) (ModelPrice, bool) {
	for _, price := range prices {
		if price.Model == model && price.Context == contextName {
			return price, true
		}
	}
	return ModelPrice{}, false
}
