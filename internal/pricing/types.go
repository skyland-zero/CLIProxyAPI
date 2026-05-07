package pricing

import (
	"context"
	"time"

	usage "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

const (
	ProviderOpenAI      = "openai"
	OpenAIPricingURL    = "https://developers.openai.com/api/docs/pricing"
	defaultPricingTable = "model_prices"
)

// ModelPrice stores a model price row normalized to USD.
type ModelPrice struct {
	Provider         string    `json:"provider"`
	Model            string    `json:"model"`
	Category         string    `json:"category"`
	Context          string    `json:"context"`
	Modality         string    `json:"modality"`
	Unit             string    `json:"unit"`
	InputPer1M       float64   `json:"input_per_1m"`
	CachedInputPer1M float64   `json:"cached_input_per_1m"`
	OutputPer1M      float64   `json:"output_per_1m"`
	TrainingPerHour  float64   `json:"training_per_hour"`
	PricePerSecond   float64   `json:"price_per_second"`
	SourceURL        string    `json:"source_url"`
	FetchedAt        time.Time `json:"fetched_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// Store persists and queries pricing rows.
type Store interface {
	UpsertPrices(ctx context.Context, prices []ModelPrice) error
	ListPrices(ctx context.Context, provider string) ([]ModelPrice, error)
	FindPrice(ctx context.Context, provider, model string) (ModelPrice, bool, error)
	Mode() string
}

// PricedEvent is a usage event with an optional USD cost estimate.
type PricedEvent struct {
	usage.Event
	EstimatedCostUSD *float64 `json:"estimated_cost_usd"`
}
