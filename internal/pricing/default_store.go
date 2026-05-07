package pricing

import (
	"context"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var defaultStore = struct {
	sync.RWMutex
	store Store
}{store: NewMemoryStore()}

func DefaultStore() Store {
	defaultStore.RLock()
	defer defaultStore.RUnlock()
	return defaultStore.store
}

func SetDefaultStore(store Store) {
	defaultStore.Lock()
	defaultStore.store = store
	defaultStore.Unlock()
}

func InitializeMemory() { SetDefaultStore(NewMemoryStore()) }

func InitializePostgres(ctx context.Context, dsn, schema string) error {
	store, err := NewPostgresStore(ctx, PostgresStoreConfig{DSN: dsn, Schema: schema})
	if err != nil {
		return err
	}
	SetDefaultStore(store)
	return nil
}

func Mode() string {
	store := DefaultStore()
	if store == nil {
		return "none"
	}
	return store.Mode()
}

func ListOpenAI(ctx context.Context) ([]ModelPrice, error) {
	store := DefaultStore()
	if store == nil {
		return nil, nil
	}
	return store.ListPrices(ctx, ProviderOpenAI)
}

func RefreshOpenAI(ctx context.Context) ([]ModelPrice, error) {
	prices, err := FetchOpenAIPrices(ctx, OpenAIPricingURL)
	if err != nil {
		return nil, err
	}
	store := DefaultStore()
	if store == nil {
		return prices, nil
	}
	if err = store.UpsertPrices(ctx, prices); err != nil {
		return nil, err
	}
	return prices, nil
}

func StartOpenAIRefreshLoop(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		refresh := func() {
			if _, err := RefreshOpenAI(ctx); err != nil {
				log.WithError(err).Warn("pricing: failed to refresh OpenAI pricing")
			}
		}
		refresh()
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
}

func providerCandidates(provider string) []string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	switch provider {
	case "openai", "codex", "openai-compat", "openai-compatible", "openai_compat", "openai-compatibility":
		return []string{ProviderOpenAI}
	default:
		return []string{provider, ProviderOpenAI}
	}
}
