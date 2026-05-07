package pricing

import (
	"context"
	"strings"
	"sync"
	"time"
)

type MemoryStore struct {
	mu     sync.RWMutex
	prices map[string]ModelPrice
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{prices: make(map[string]ModelPrice)}
}

func (s *MemoryStore) Mode() string { return "memory" }

func (s *MemoryStore) UpsertPrices(_ context.Context, prices []ModelPrice) error {
	if s == nil {
		return nil
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, price := range prices {
		price = normalizePrice(price, now)
		if price.Provider == "" || price.Model == "" {
			continue
		}
		s.prices[priceKey(price.Provider, price.Model, price.Category, price.Context, price.Modality, price.Unit)] = price
	}
	return nil
}

func (s *MemoryStore) ListPrices(_ context.Context, provider string) ([]ModelPrice, error) {
	if s == nil {
		return nil, nil
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ModelPrice, 0, len(s.prices))
	for _, price := range s.prices {
		if provider == "" || price.Provider == provider {
			out = append(out, price)
		}
	}
	return out, nil
}

func (s *MemoryStore) FindPrice(_ context.Context, provider, model string) (ModelPrice, bool, error) {
	if s == nil {
		return ModelPrice{}, false, nil
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return ModelPrice{}, false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, candidate := range providerCandidates(provider) {
		best, ok := bestPriceForModel(s.prices, candidate, model)
		if ok {
			return best, true, nil
		}
	}
	return ModelPrice{}, false, nil
}

func bestPriceForModel(prices map[string]ModelPrice, provider, model string) (ModelPrice, bool) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	for _, preferredContext := range []string{"short_context", "standard", ""} {
		for _, price := range prices {
			if price.Provider == provider && price.Model == model && price.Unit == "1m_tokens" && price.Modality == "text" {
				if preferredContext == "" || price.Context == preferredContext {
					return price, true
				}
			}
		}
	}
	return ModelPrice{}, false
}

func priceKey(provider, model, category, contextName, modality, unit string) string {
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(provider)),
		strings.TrimSpace(model),
		strings.ToLower(strings.TrimSpace(category)),
		strings.ToLower(strings.TrimSpace(contextName)),
		strings.ToLower(strings.TrimSpace(modality)),
		strings.ToLower(strings.TrimSpace(unit)),
	}, "\x00")
}

func normalizePrice(price ModelPrice, now time.Time) ModelPrice {
	price.Provider = strings.ToLower(strings.TrimSpace(price.Provider))
	if price.Provider == "" {
		price.Provider = ProviderOpenAI
	}
	price.Model = strings.TrimSpace(price.Model)
	price.Category = strings.ToLower(strings.TrimSpace(price.Category))
	if price.Category == "" {
		price.Category = "standard"
	}
	price.Context = strings.ToLower(strings.TrimSpace(price.Context))
	if price.Context == "" {
		price.Context = "standard"
	}
	price.Modality = strings.ToLower(strings.TrimSpace(price.Modality))
	if price.Modality == "" {
		price.Modality = "text"
	}
	price.Unit = strings.ToLower(strings.TrimSpace(price.Unit))
	if price.Unit == "" {
		price.Unit = "1m_tokens"
	}
	if price.SourceURL == "" {
		price.SourceURL = OpenAIPricingURL
	}
	if price.FetchedAt.IsZero() {
		price.FetchedAt = now
	}
	if price.UpdatedAt.IsZero() {
		price.UpdatedAt = now
	}
	return price
}
