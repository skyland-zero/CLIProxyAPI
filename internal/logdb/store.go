package logdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	appLogTable         = "app_log_entries"
	requestLogTable     = "request_log_entries"
	requestContentTable = "request_log_contents"
)

func (m *Manager) ensureSchema(ctx context.Context) error {
	if m == nil || m.pool == nil {
		return fmt.Errorf("logdb: not initialized")
	}
	if m.schema != "" {
		if _, err := m.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdentifier(m.schema)); err != nil {
			return fmt.Errorf("logdb: create schema: %w", err)
		}
	}
	if _, err := m.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			timestamp TIMESTAMPTZ NOT NULL,
			level TEXT NOT NULL,
			log_kind TEXT NOT NULL,
			request_id TEXT NOT NULL,
			message TEXT NOT NULL,
			provider TEXT NOT NULL,
			model TEXT NOT NULL,
			path TEXT NOT NULL,
			method TEXT NOT NULL,
			http_status INTEGER NOT NULL DEFAULT 0,
			source_file TEXT NOT NULL,
			source_line INTEGER NOT NULL DEFAULT 0,
			fields_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, m.schemaTable(appLogTable))); err != nil {
		return fmt.Errorf("logdb: create app log table: %w", err)
	}
	if _, err := m.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			request_id TEXT NOT NULL,
			timestamp TIMESTAMPTZ NOT NULL,
			endpoint TEXT NOT NULL,
			method TEXT NOT NULL,
			provider TEXT NOT NULL,
			model TEXT NOT NULL,
			alias TEXT NOT NULL,
			auth_id TEXT NOT NULL,
			auth_type TEXT NOT NULL,
			status_code INTEGER NOT NULL DEFAULT 0,
			failed BOOLEAN NOT NULL,
			has_api_error BOOLEAN NOT NULL,
			latency_ms BIGINT NOT NULL DEFAULT 0,
			downstream_transport TEXT NOT NULL,
			upstream_transport TEXT NOT NULL,
			request_bytes BIGINT NOT NULL DEFAULT 0,
			response_bytes BIGINT NOT NULL DEFAULT 0,
			content_bytes BIGINT NOT NULL DEFAULT 0,
			has_websocket_timeline BOOLEAN NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, m.schemaTable(requestLogTable))); err != nil {
		return fmt.Errorf("logdb: create request log table: %w", err)
	}
	if _, err := m.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			request_log_id TEXT PRIMARY KEY,
			request_id TEXT NOT NULL,
			content_gzip BYTEA NOT NULL,
			size_uncompressed BIGINT NOT NULL DEFAULT 0,
			schema_version INTEGER NOT NULL DEFAULT 1,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, m.schemaTable(requestContentTable))); err != nil {
		return fmt.Errorf("logdb: create request content table: %w", err)
	}
	indexStatements := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (timestamp DESC, id DESC)", quoteIdentifier("app_log_entries_timestamp_id_idx"), m.schemaTable(appLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (level, timestamp DESC, id DESC)", quoteIdentifier("app_log_entries_level_timestamp_id_idx"), m.schemaTable(appLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (request_id)", quoteIdentifier("app_log_entries_request_id_idx"), m.schemaTable(appLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (provider, model, timestamp DESC, id DESC)", quoteIdentifier("app_log_entries_provider_model_timestamp_id_idx"), m.schemaTable(appLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (path, timestamp DESC, id DESC)", quoteIdentifier("app_log_entries_path_timestamp_id_idx"), m.schemaTable(appLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (timestamp DESC, id DESC)", quoteIdentifier("request_log_entries_timestamp_id_idx"), m.schemaTable(requestLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (request_id, timestamp DESC, id DESC)", quoteIdentifier("request_log_entries_request_id_timestamp_id_idx"), m.schemaTable(requestLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (provider, model, timestamp DESC, id DESC)", quoteIdentifier("request_log_entries_provider_model_timestamp_id_idx"), m.schemaTable(requestLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (endpoint, timestamp DESC, id DESC)", quoteIdentifier("request_log_entries_endpoint_timestamp_id_idx"), m.schemaTable(requestLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (status_code, timestamp DESC, id DESC)", quoteIdentifier("request_log_entries_status_code_timestamp_id_idx"), m.schemaTable(requestLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (timestamp DESC, id DESC) WHERE failed", quoteIdentifier("request_log_entries_failed_timestamp_id_idx"), m.schemaTable(requestLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (auth_id, timestamp DESC, id DESC)", quoteIdentifier("request_log_entries_auth_id_timestamp_id_idx"), m.schemaTable(requestLogTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (request_id)", quoteIdentifier("request_log_contents_request_id_idx"), m.schemaTable(requestContentTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (request_id, created_at DESC)", quoteIdentifier("request_log_contents_request_id_created_at_idx"), m.schemaTable(requestContentTable)),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (created_at)", quoteIdentifier("request_log_contents_created_at_idx"), m.schemaTable(requestContentTable)),
	}
	for _, stmt := range indexStatements {
		if _, err := m.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("logdb: create index: %w", err)
		}
	}
	return nil
}

func (m *Manager) start(cfg Config) {
	m.wg.Add(3)
	go m.runAppLogWorker(cfg.AppLogBatchSize)
	go m.runRequestLogWorker(cfg.RequestLogBatchSize)
	go m.runPruner()
}

func (m *Manager) runAppLogWorker(batchSize int) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()
	batch := make([]AppLogEntry, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := m.flushContext(15 * time.Second)
		if err := m.flushAppLogs(ctx, batch); err != nil {
			m.writeError("logdb: flush app logs: %v", err)
		}
		cancel()
		clear(batch)
		batch = batch[:0]
	}
	for {
		select {
		case <-m.stopCh:
			for {
				select {
				case item := <-m.appLogCh:
					batch = append(batch, item)
					if len(batch) >= batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case item := <-m.appLogCh:
			batch = append(batch, item)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (m *Manager) runRequestLogWorker(batchSize int) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()
	batch := make([]requestLogQueueItem, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		flushedBatch := batch
		ctx, cancel := m.flushContext(20 * time.Second)
		if err := m.flushRequestLogs(ctx, flushedBatch); err != nil {
			m.writeError("logdb: flush request logs: %v", err)
		}
		cancel()
		m.releaseRequestLogBatch(flushedBatch)
		clear(flushedBatch)
		batch = batch[:0]
	}
	for {
		select {
		case <-m.stopCh:
			for {
				select {
				case item := <-m.requestLogCh:
					batch = append(batch, item)
					if len(batch) >= batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case item := <-m.requestLogCh:
			batch = append(batch, item)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (m *Manager) releaseRequestLogBatch(entries []requestLogQueueItem) {
	for _, item := range entries {
		m.releaseRequestLogSlot(item.queueBytes)
	}
}

func (m *Manager) runPruner() {
	defer m.wg.Done()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			if err := m.prune(ctx); err != nil {
				m.writeError("logdb: prune logs: %v", err)
			}
			cancel()
		}
	}
}

func (m *Manager) prune(ctx context.Context) error {
	cutoff := time.Now().UTC().Add(-m.retention)
	const pruneBatchSize = 1000
	queries := []string{
		fmt.Sprintf("DELETE FROM %s WHERE ctid IN (SELECT ctid FROM %s WHERE created_at < $1 LIMIT %d)", m.schemaTable(requestContentTable), m.schemaTable(requestContentTable), pruneBatchSize),
		fmt.Sprintf("DELETE FROM %s WHERE ctid IN (SELECT ctid FROM %s WHERE timestamp < $1 LIMIT %d)", m.schemaTable(requestLogTable), m.schemaTable(requestLogTable), pruneBatchSize),
		fmt.Sprintf("DELETE FROM %s WHERE ctid IN (SELECT ctid FROM %s WHERE timestamp < $1 LIMIT %d)", m.schemaTable(appLogTable), m.schemaTable(appLogTable), pruneBatchSize),
	}
	for _, query := range queries {
		for {
			result, err := m.pool.Exec(ctx, query, cutoff)
			if err != nil {
				if strings.Contains(err.Error(), "context canceled") {
					return nil
				}
				return err
			}
			if result.RowsAffected() < pruneBatchSize {
				break
			}
		}
	}
	return nil
}

func (m *Manager) flushAppLogs(ctx context.Context, entries []AppLogEntry) error {
	rows := make([][]any, 0, len(entries))
	for _, entry := range entries {
		rows = append(rows, []any{entry.ID, entry.Timestamp, entry.Level, entry.LogKind, entry.RequestID, entry.Message, entry.Provider, entry.Model, entry.Path, entry.Method, entry.HTTPStatus, entry.SourceFile, entry.SourceLine, string(entry.FieldsJSON)})
	}
	_, err := m.pool.CopyFrom(ctx, m.tableIdentifier(appLogTable), []string{"id", "timestamp", "level", "log_kind", "request_id", "message", "provider", "model", "path", "method", "http_status", "source_file", "source_line", "fields_json"}, pgx.CopyFromRows(rows))
	return err
}

func (m *Manager) flushRequestLogs(ctx context.Context, entries []requestLogQueueItem) error {
	logEntries := make([]RequestLogEntry, 0, len(entries))
	contentEntries := make([]RequestLogContentRow, 0, len(entries))
	for _, item := range entries {
		responseBody := item.response
		if decoded, truncated, err := decompressResponseLimited(item.responseHeaders, item.response, maxRequestLogStreamingBytes); err == nil {
			responseBody = decoded
			item.truncationFlags.responseBody = item.truncationFlags.responseBody || truncated
		} else if responseHasContentEncoding(item.responseHeaders) {
			responseBody = nil
			item.truncationFlags.responseBody = true
		}
		_, entry, contentRow, errBuild := buildRequestLogRecord(item.url, item.method, item.requestHeaders, item.body, item.requestBytes, item.statusCode, item.responseHeaders, responseBody, item.responseBytes, item.websocketTimeline, item.apiRequest, item.apiResponse, item.apiWebsocketTimeline, item.apiResponseErrors, item.requestID, item.requestTimestamp, item.apiResponseTimestamp, item.truncationFlags)
		if errBuild != nil {
			m.writeError("logdb: build request log: %v", errBuild)
			continue
		}
		logEntries = append(logEntries, entry)
		contentEntries = append(contentEntries, contentRow)
	}
	if len(logEntries) == 0 {
		return nil
	}
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows := make([][]any, 0, len(logEntries))
	for _, entry := range logEntries {
		rows = append(rows, []any{entry.ID, entry.RequestID, entry.Timestamp, entry.Endpoint, entry.Method, entry.Provider, entry.Model, entry.Alias, entry.AuthID, entry.AuthType, entry.StatusCode, entry.Failed, entry.HasAPIError, entry.LatencyMs, entry.DownstreamTransport, entry.UpstreamTransport, entry.RequestBytes, entry.ResponseBytes, entry.ContentBytes, entry.HasWebsocketTimeline})
	}
	if _, err = tx.CopyFrom(ctx, m.tableIdentifier(requestLogTable), []string{"id", "request_id", "timestamp", "endpoint", "method", "provider", "model", "alias", "auth_id", "auth_type", "status_code", "failed", "has_api_error", "latency_ms", "downstream_transport", "upstream_transport", "request_bytes", "response_bytes", "content_bytes", "has_websocket_timeline"}, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	if err = m.flushRequestContentsTx(ctx, tx, contentEntries); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (m *Manager) flushRequestContentsTx(ctx context.Context, tx pgx.Tx, entries []RequestLogContentRow) error {
	rows := make([][]any, 0, len(entries))
	for _, entry := range entries {
		rows = append(rows, []any{entry.RequestLogID, entry.RequestID, entry.ContentGzip, entry.SizeUncompressed, entry.SchemaVersion})
	}
	_, err := tx.CopyFrom(ctx, m.tableIdentifier(requestContentTable), []string{"request_log_id", "request_id", "content_gzip", "size_uncompressed", "schema_version"}, pgx.CopyFromRows(rows))
	return err
}

func (m *Manager) tableIdentifier(table string) pgx.Identifier {
	if m == nil || strings.TrimSpace(m.schema) == "" {
		return pgx.Identifier{table}
	}
	return pgx.Identifier{strings.TrimSpace(m.schema), table}
}
