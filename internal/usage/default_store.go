package usage

import (
	"context"
	"sync"
)

var defaultStore = struct {
	sync.RWMutex
	store Store
}{store: NewMemoryStore()}

// DefaultStore returns the active usage store.
func DefaultStore() Store {
	defaultStore.RLock()
	defer defaultStore.RUnlock()
	return defaultStore.store
}

// SetDefaultStore replaces the active usage store.
func SetDefaultStore(store Store) {
	defaultStore.Lock()
	defaultStore.store = store
	defaultStore.Unlock()
}

// InitializeMemory configures the built-in tracker to keep recent events in memory only.
func InitializeMemory() {
	SetDefaultStore(NewMemoryStore())
}

// InitializePostgres configures the built-in tracker to persist recent events in PostgreSQL only.
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

func Snapshot(ctx context.Context, query Query) (StatisticsSnapshot, error) {
	store := DefaultStore()
	if store == nil {
		return StatisticsSnapshot{}, nil
	}
	return store.Snapshot(ctx, query)
}

func Events(ctx context.Context, query Query) ([]Event, int64, error) {
	store := DefaultStore()
	if store == nil {
		return nil, 0, nil
	}
	return store.Events(ctx, query)
}

func Summary(ctx context.Context, query SummaryQuery) ([]SummaryRow, error) {
	store := DefaultStore()
	if store == nil {
		return nil, nil
	}
	return store.Summary(ctx, query)
}

func Delete(ctx context.Context, query Query) (int64, error) {
	store := DefaultStore()
	if store == nil {
		return 0, nil
	}
	return store.Delete(ctx, query)
}
