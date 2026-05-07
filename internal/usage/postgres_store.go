package usage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultUsageTable = "usage_events"

type PostgresStoreConfig struct {
	DSN    string
	Schema string
}

// PostgresStore persists recent usage events in PostgreSQL only.
type PostgresStore struct {
	db        *sql.DB
	schema    string
	tableName string
	mu        sync.Mutex
	lastPrune time.Time
}

func NewPostgresStore(ctx context.Context, cfg PostgresStoreConfig) (*PostgresStore, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, fmt.Errorf("usage postgres store: DSN is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("usage postgres store: open database: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("usage postgres store: ping database: %w", err)
	}
	store := &PostgresStore{
		db:        db,
		schema:    strings.TrimSpace(cfg.Schema),
		tableName: defaultUsageTable,
	}
	if err = store.EnsureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err = store.prune(ctx, nowUTC()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) Mode() string { return "postgres" }

func (s *PostgresStore) Record(ctx context.Context, event Event) error {
	if s == nil || s.db == nil {
		return nil
	}
	now := nowUTC()
	if event.Timestamp.IsZero() {
		event.Timestamp = now
	}
	event.Timestamp = event.Timestamp.UTC()
	if event.Timestamp.Before(retentionCutoff(now)) {
		return nil
	}
	if err := s.pruneIfDue(ctx, now); err != nil {
		return err
	}
	raw := json.RawMessage(`{}`)
	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, timestamp, request_id, endpoint, provider, model, alias,
			auth_id, auth_index, auth_type, source, api_key_hash, latency_ms, failed,
			input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, raw, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20, NOW()
		)
		ON CONFLICT (id) DO NOTHING
	`, s.fullTableName())
	_, err := s.db.ExecContext(ctx, query,
		event.ID,
		event.Timestamp,
		event.RequestID,
		event.Endpoint,
		event.Provider,
		event.Model,
		event.Alias,
		event.AuthID,
		event.AuthIndex,
		event.AuthType,
		event.Source,
		event.APIKeyHash,
		event.LatencyMs,
		event.Failed,
		event.Tokens.InputTokens,
		event.Tokens.OutputTokens,
		event.Tokens.ReasoningTokens,
		event.Tokens.CachedTokens,
		event.Tokens.TotalTokens,
		raw,
	)
	if err != nil {
		return fmt.Errorf("usage postgres store: insert event: %w", err)
	}
	return nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, query Query) (StatisticsSnapshot, error) {
	events, err := s.queryEvents(ctx, query, false)
	if err != nil {
		return StatisticsSnapshot{}, err
	}
	return buildSnapshot(events), nil
}

func (s *PostgresStore) Events(ctx context.Context, query Query) ([]Event, int64, error) {
	events, total, err := s.queryEventsPage(ctx, query)
	if err != nil {
		return nil, 0, err
	}
	return events, total, nil
}

func (s *PostgresStore) Summary(ctx context.Context, query SummaryQuery) ([]SummaryRow, error) {
	events, err := s.queryEvents(ctx, query.Query, false)
	if err != nil {
		return nil, err
	}
	return buildSummary(events, query.GroupBy), nil
}

func (s *PostgresStore) Delete(ctx context.Context, query Query) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	query = clampQueryToRetention(query, nowUTC())
	where, args := s.whereClause(query)
	deleteQuery := fmt.Sprintf("DELETE FROM %s %s", s.fullTableName(), where)
	result, err := s.db.ExecContext(ctx, deleteQuery, args...)
	if err != nil {
		return 0, fmt.Errorf("usage postgres store: delete events: %w", err)
	}
	rows, _ := result.RowsAffected()
	return rows, nil
}

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("usage postgres store: not initialized")
	}
	if s.schema != "" {
		if _, err := s.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdentifier(s.schema)); err != nil {
			return fmt.Errorf("usage postgres store: create schema: %w", err)
		}
	}
	table := s.fullTableName()
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			timestamp TIMESTAMPTZ NOT NULL,
			request_id TEXT NOT NULL,
			endpoint TEXT NOT NULL,
			provider TEXT NOT NULL,
			model TEXT NOT NULL,
			alias TEXT NOT NULL,
			auth_id TEXT NOT NULL,
			auth_index TEXT NOT NULL,
			auth_type TEXT NOT NULL,
			source TEXT NOT NULL,
			api_key_hash TEXT NOT NULL,
			latency_ms BIGINT NOT NULL,
			failed BOOLEAN NOT NULL,
			input_tokens BIGINT NOT NULL DEFAULT 0,
			output_tokens BIGINT NOT NULL DEFAULT 0,
			reasoning_tokens BIGINT NOT NULL DEFAULT 0,
			cached_tokens BIGINT NOT NULL DEFAULT 0,
			total_tokens BIGINT NOT NULL DEFAULT 0,
			raw JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, table)); err != nil {
		return fmt.Errorf("usage postgres store: create usage table: %w", err)
	}
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (timestamp DESC)", quoteIdentifier("usage_events_timestamp_idx"), table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (provider, model)", quoteIdentifier("usage_events_provider_model_idx"), table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (api_key_hash)", quoteIdentifier("usage_events_api_key_hash_idx"), table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (auth_id)", quoteIdentifier("usage_events_auth_id_idx"), table),
	}
	for _, stmt := range indexes {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("usage postgres store: create index: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) queryEventsPage(ctx context.Context, query Query) ([]Event, int64, error) {
	query = clampQueryToRetention(query, nowUTC())
	where, args := s.whereClause(query)
	var total int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", s.fullTableName(), where)
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("usage postgres store: count events: %w", err)
	}
	selectArgs := append([]any(nil), args...)
	limitPos := len(selectArgs) + 1
	selectArgs = append(selectArgs, query.Limit)
	offsetPos := len(selectArgs) + 1
	selectArgs = append(selectArgs, query.Offset)
	selectQuery := fmt.Sprintf(`
		SELECT id, timestamp, request_id, endpoint, provider, model, alias,
			auth_id, auth_index, auth_type, source, api_key_hash, latency_ms, failed,
			input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens
		FROM %s %s
		ORDER BY timestamp DESC, id DESC
		LIMIT $%d OFFSET $%d
	`, s.fullTableName(), where, limitPos, offsetPos)
	events, err := s.scanEvents(ctx, selectQuery, selectArgs...)
	return events, total, err
}

func (s *PostgresStore) queryEvents(ctx context.Context, query Query, order bool) ([]Event, error) {
	query = clampQueryToRetention(query, nowUTC())
	where, args := s.whereClause(query)
	orderBy := ""
	if order {
		orderBy = " ORDER BY timestamp DESC, id DESC"
	}
	selectQuery := fmt.Sprintf(`
		SELECT id, timestamp, request_id, endpoint, provider, model, alias,
			auth_id, auth_index, auth_type, source, api_key_hash, latency_ms, failed,
			input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens
		FROM %s %s%s
	`, s.fullTableName(), where, orderBy)
	return s.scanEvents(ctx, selectQuery, args...)
}

func (s *PostgresStore) scanEvents(ctx context.Context, query string, args ...any) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("usage postgres store: query events: %w", err)
	}
	defer rows.Close()
	events := make([]Event, 0, 128)
	for rows.Next() {
		var event Event
		if err = rows.Scan(
			&event.ID,
			&event.Timestamp,
			&event.RequestID,
			&event.Endpoint,
			&event.Provider,
			&event.Model,
			&event.Alias,
			&event.AuthID,
			&event.AuthIndex,
			&event.AuthType,
			&event.Source,
			&event.APIKeyHash,
			&event.LatencyMs,
			&event.Failed,
			&event.Tokens.InputTokens,
			&event.Tokens.OutputTokens,
			&event.Tokens.ReasoningTokens,
			&event.Tokens.CachedTokens,
			&event.Tokens.TotalTokens,
		); err != nil {
			return nil, fmt.Errorf("usage postgres store: scan event: %w", err)
		}
		event.Timestamp = event.Timestamp.UTC()
		event.Tokens = normalizeTokens(event.Tokens)
		events = append(events, event)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("usage postgres store: iterate events: %w", err)
	}
	return events, nil
}

func (s *PostgresStore) whereClause(query Query) (string, []any) {
	clauses := []string{"timestamp >= $1", "timestamp <= $2"}
	args := []any{query.From, query.To}
	addString := func(column, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	addString("provider", strings.TrimSpace(query.Provider))
	addString("model", strings.TrimSpace(query.Model))
	addString("alias", strings.TrimSpace(query.Alias))
	addString("auth_id", strings.TrimSpace(query.AuthID))
	addString("auth_type", strings.TrimSpace(query.AuthType))
	addString("source", strings.TrimSpace(query.Source))
	addString("api_key_hash", strings.TrimSpace(query.APIKeyHash))
	if query.Failed != nil {
		args = append(args, *query.Failed)
		clauses = append(clauses, fmt.Sprintf("failed = $%d", len(args)))
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func (s *PostgresStore) pruneIfDue(ctx context.Context, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.lastPrune.IsZero() && now.Sub(s.lastPrune) < time.Hour {
		return nil
	}
	if err := s.prune(ctx, now); err != nil {
		return err
	}
	s.lastPrune = now
	return nil
}

func (s *PostgresStore) prune(ctx context.Context, now time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE timestamp < $1", s.fullTableName()), retentionCutoff(now))
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("usage postgres store: prune old events: %w", err)
	}
	return nil
}

func (s *PostgresStore) fullTableName() string {
	if strings.TrimSpace(s.schema) == "" {
		return quoteIdentifier(s.tableName)
	}
	return quoteIdentifier(s.schema) + "." + quoteIdentifier(s.tableName)
}

func quoteIdentifier(identifier string) string {
	replaced := strings.ReplaceAll(identifier, "\"", "\"\"")
	return "\"" + replaced + "\""
}
