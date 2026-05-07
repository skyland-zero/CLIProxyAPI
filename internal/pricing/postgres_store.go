package pricing

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresStoreConfig struct {
	DSN    string
	Schema string
}

type PostgresStore struct {
	db     *sql.DB
	schema string
	table  string
}

func NewPostgresStore(ctx context.Context, cfg PostgresStoreConfig) (*PostgresStore, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, fmt.Errorf("pricing postgres store: DSN is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("pricing postgres store: open database: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pricing postgres store: ping database: %w", err)
	}
	store := &PostgresStore{db: db, schema: strings.TrimSpace(cfg.Schema), table: defaultPricingTable}
	if err = store.EnsureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) Mode() string { return "postgres" }

func (s *PostgresStore) UpsertPrices(ctx context.Context, prices []ModelPrice) error {
	if s == nil || s.db == nil {
		return nil
	}
	now := time.Now().UTC()
	query := fmt.Sprintf(`
		INSERT INTO %s (
			provider, model, category, context, modality, unit,
			input_per_1m, cached_input_per_1m, output_per_1m,
			training_per_hour, price_per_second, source_url, fetched_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11, $12, $13, NOW()
		)
		ON CONFLICT (provider, model, category, context, modality, unit)
		DO UPDATE SET
			input_per_1m = EXCLUDED.input_per_1m,
			cached_input_per_1m = EXCLUDED.cached_input_per_1m,
			output_per_1m = EXCLUDED.output_per_1m,
			training_per_hour = EXCLUDED.training_per_hour,
			price_per_second = EXCLUDED.price_per_second,
			source_url = EXCLUDED.source_url,
			fetched_at = EXCLUDED.fetched_at,
			updated_at = NOW()
	`, s.fullTableName())
	for _, price := range prices {
		price = normalizePrice(price, now)
		if price.Model == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, query,
			price.Provider,
			price.Model,
			price.Category,
			price.Context,
			price.Modality,
			price.Unit,
			price.InputPer1M,
			price.CachedInputPer1M,
			price.OutputPer1M,
			price.TrainingPerHour,
			price.PricePerSecond,
			price.SourceURL,
			price.FetchedAt,
		); err != nil {
			return fmt.Errorf("pricing postgres store: upsert price: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) ListPrices(ctx context.Context, provider string) ([]ModelPrice, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	query := fmt.Sprintf(`
		SELECT provider, model, category, context, modality, unit,
			input_per_1m, cached_input_per_1m, output_per_1m,
			training_per_hour, price_per_second, source_url, fetched_at, updated_at
		FROM %s
	`, s.fullTableName())
	args := []any{}
	if provider != "" {
		query += " WHERE provider = $1"
		args = append(args, provider)
	}
	query += " ORDER BY provider, model, context, modality"
	return s.scanPrices(ctx, query, args...)
}

func (s *PostgresStore) FindPrice(ctx context.Context, provider, model string) (ModelPrice, bool, error) {
	if s == nil || s.db == nil {
		return ModelPrice{}, false, nil
	}
	model = strings.TrimSpace(model)
	for _, candidate := range providerCandidates(provider) {
		prices, err := s.scanPrices(ctx, fmt.Sprintf(`
			SELECT provider, model, category, context, modality, unit,
				input_per_1m, cached_input_per_1m, output_per_1m,
				training_per_hour, price_per_second, source_url, fetched_at, updated_at
			FROM %s
			WHERE provider = $1 AND model = $2 AND unit = '1m_tokens' AND modality = 'text'
			ORDER BY CASE context WHEN 'short_context' THEN 0 WHEN 'standard' THEN 1 ELSE 2 END
			LIMIT 1
		`, s.fullTableName()), candidate, model)
		if err != nil {
			return ModelPrice{}, false, err
		}
		if len(prices) > 0 {
			return prices[0], true, nil
		}
	}
	return ModelPrice{}, false, nil
}

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("pricing postgres store: not initialized")
	}
	if s.schema != "" {
		if _, err := s.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdentifier(s.schema)); err != nil {
			return fmt.Errorf("pricing postgres store: create schema: %w", err)
		}
	}
	table := s.fullTableName()
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			provider TEXT NOT NULL,
			model TEXT NOT NULL,
			category TEXT NOT NULL,
			context TEXT NOT NULL,
			modality TEXT NOT NULL,
			unit TEXT NOT NULL,
			input_per_1m NUMERIC NOT NULL DEFAULT 0,
			cached_input_per_1m NUMERIC NOT NULL DEFAULT 0,
			output_per_1m NUMERIC NOT NULL DEFAULT 0,
			training_per_hour NUMERIC NOT NULL DEFAULT 0,
			price_per_second NUMERIC NOT NULL DEFAULT 0,
			source_url TEXT NOT NULL,
			fetched_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (provider, model, category, context, modality, unit)
		)
	`, table)); err != nil {
		return fmt.Errorf("pricing postgres store: create table: %w", err)
	}
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (provider, model)", quoteIdentifier("model_prices_provider_model_idx"), table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (fetched_at DESC)", quoteIdentifier("model_prices_fetched_at_idx"), table),
	}
	for _, stmt := range indexes {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("pricing postgres store: create index: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) scanPrices(ctx context.Context, query string, args ...any) ([]ModelPrice, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pricing postgres store: query prices: %w", err)
	}
	defer rows.Close()
	prices := make([]ModelPrice, 0, 32)
	for rows.Next() {
		var price ModelPrice
		if err = rows.Scan(
			&price.Provider,
			&price.Model,
			&price.Category,
			&price.Context,
			&price.Modality,
			&price.Unit,
			&price.InputPer1M,
			&price.CachedInputPer1M,
			&price.OutputPer1M,
			&price.TrainingPerHour,
			&price.PricePerSecond,
			&price.SourceURL,
			&price.FetchedAt,
			&price.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("pricing postgres store: scan price: %w", err)
		}
		prices = append(prices, price)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("pricing postgres store: iterate prices: %w", err)
	}
	return prices, nil
}

func (s *PostgresStore) fullTableName() string {
	if strings.TrimSpace(s.schema) == "" {
		return quoteIdentifier(s.table)
	}
	return quoteIdentifier(s.schema) + "." + quoteIdentifier(s.table)
}

func quoteIdentifier(identifier string) string {
	replaced := strings.ReplaceAll(identifier, "\"", "\"\"")
	return "\"" + replaced + "\""
}
