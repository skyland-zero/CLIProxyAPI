package logdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type AppLogEntry struct {
	ID         string
	Timestamp  time.Time
	Level      string
	LogKind    string
	RequestID  string
	Message    string
	Provider   string
	Model      string
	Path       string
	Method     string
	HTTPStatus int32
	SourceFile string
	SourceLine int32
	FieldsJSON []byte
}

type AppLogQuery struct {
	From         time.Time
	To           time.Time
	Level        string
	LogKind      string
	RequestID    string
	Provider     string
	Model        string
	Path         string
	Method       string
	HTTPStatus   *int
	StatusMin    *int
	StatusMax    *int
	SourceFile   string
	Message      string
	Limit        int
	Offset       int
	IncludeTotal bool
}

type AppLogRecord struct {
	ID         string         `json:"id"`
	Timestamp  time.Time      `json:"timestamp"`
	Level      string         `json:"level"`
	LogKind    string         `json:"log_kind"`
	RequestID  string         `json:"request_id"`
	Message    string         `json:"message"`
	Provider   string         `json:"provider"`
	Model      string         `json:"model"`
	Path       string         `json:"path"`
	Method     string         `json:"method"`
	HTTPStatus int            `json:"http_status"`
	SourceFile string         `json:"source_file"`
	SourceLine int            `json:"source_line"`
	Fields     map[string]any `json:"fields"`
}

type AppLogHook struct{}

var appLogHookOnce sync.Once

func NewAppLogHook() *AppLogHook { return &AppLogHook{} }

func RegisterAppLogHook() {
	appLogHookOnce.Do(func() {
		log.AddHook(NewAppLogHook())
	})
}

func (h *AppLogHook) Levels() []log.Level {
	return []log.Level{log.InfoLevel, log.WarnLevel, log.ErrorLevel, log.FatalLevel, log.PanicLevel}
}

func (h *AppLogHook) Fire(entry *log.Entry) error {
	mgr := DefaultManager()
	if mgr == nil || mgr.closed.Load() {
		return nil
	}
	record := buildAppLogEntry(entry)
	mgr.enqueueMu.RLock()
	defer mgr.enqueueMu.RUnlock()
	if mgr.closed.Load() {
		return nil
	}
	select {
	case mgr.appLogCh <- record:
	default:
	}
	return nil
}

func buildAppLogEntry(entry *log.Entry) AppLogEntry {
	fields := sanitizeFields(entry.Data)
	fieldsJSON, _ := json.Marshal(fields)
	if len(fieldsJSON) > 32*1024 {
		fieldsJSON = []byte(`{"truncated":true}`)
	}
	requestID, _ := fields["request_id"].(string)
	provider, _ := fields["provider"].(string)
	model, _ := fields["model"].(string)
	path, _ := fields["path"].(string)
	method, _ := fields["method"].(string)
	logKind, _ := fields["log_kind"].(string)
	status := int32(0)
	if raw, ok := fields["http_status"]; ok {
		switch v := raw.(type) {
		case int:
			status = int32(v)
		case int32:
			status = v
		case int64:
			status = int32(v)
		case float64:
			status = int32(v)
		}
	}
	record := AppLogEntry{
		ID:         uuid.NewString(),
		Timestamp:  entry.Time.UTC(),
		Level:      entry.Level.String(),
		LogKind:    strings.TrimSpace(logKind),
		RequestID:  strings.TrimSpace(requestID),
		Message:    strings.TrimSpace(entry.Message),
		Provider:   strings.TrimSpace(provider),
		Model:      strings.TrimSpace(model),
		Path:       strings.TrimSpace(path),
		Method:     strings.TrimSpace(method),
		HTTPStatus: status,
		FieldsJSON: fieldsJSON,
	}
	if record.LogKind == "" {
		record.LogKind = "application"
	}
	if entry.Caller != nil {
		record.SourceFile = entry.Caller.File
		record.SourceLine = int32(entry.Caller.Line)
	}
	return record
}

func sanitizeFields(data log.Fields) map[string]any {
	out := make(map[string]any, len(data))
	for key, value := range data {
		out[key] = sanitizeFieldValue(value)
	}
	return out
}

func sanitizeFieldValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		return v
	case bool:
		return v
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int64(v)
	case uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return float64(v)
	case float64:
		return v
	case error:
		return v.Error()
	case time.Duration:
		return v.String()
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case fmt.Stringer:
		return v.String()
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		var decoded any
		if err = json.Unmarshal(raw, &decoded); err != nil {
			return string(raw)
		}
		return decoded
	}
}

func QueryAppLogs(ctx context.Context, query AppLogQuery) ([]AppLogRecord, int64, error) {
	mgr := DefaultManager()
	if mgr == nil {
		return nil, 0, nil
	}
	return mgr.queryAppLogs(ctx, query)
}

func (m *Manager) queryAppLogs(ctx context.Context, query AppLogQuery) ([]AppLogRecord, int64, error) {
	query = normalizeAppLogQuery(query)
	where, args := appLogWhereClause(query)
	total := int64(-1)
	if query.IncludeTotal {
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", m.schemaTable(appLogTable), where)
		if err := m.pool.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
			return nil, 0, err
		}
	}
	selectArgs := append([]any(nil), args...)
	selectArgs = append(selectArgs, query.Limit, query.Offset)
	querySQL := fmt.Sprintf(`
		SELECT id, timestamp, level, log_kind, request_id, message, provider, model, path, method, http_status, source_file, source_line, fields_json
		FROM %s %s
		ORDER BY timestamp DESC, id DESC
		LIMIT $%d OFFSET $%d
	`, m.schemaTable(appLogTable), where, len(selectArgs)-1, len(selectArgs))
	rows, err := m.pool.Query(ctx, querySQL, selectArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := make([]AppLogRecord, 0, query.Limit)
	for rows.Next() {
		var record AppLogRecord
		var fieldsJSON []byte
		if err = rows.Scan(&record.ID, &record.Timestamp, &record.Level, &record.LogKind, &record.RequestID, &record.Message, &record.Provider, &record.Model, &record.Path, &record.Method, &record.HTTPStatus, &record.SourceFile, &record.SourceLine, &fieldsJSON); err != nil {
			return nil, 0, err
		}
		if len(fieldsJSON) > 0 {
			_ = json.Unmarshal(fieldsJSON, &record.Fields)
		}
		record.Timestamp = record.Timestamp.UTC()
		out = append(out, record)
	}
	return out, total, rows.Err()
}

func normalizeAppLogQuery(query AppLogQuery) AppLogQuery {
	now := time.Now().UTC()
	if query.To.IsZero() || query.To.After(now) {
		query.To = now
	}
	if query.From.IsZero() || query.From.After(query.To) {
		query.From = now.Add(-defaultRetention)
	}
	if query.Limit <= 0 || query.Limit > 1000 {
		query.Limit = 100
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	if query.Offset > 100000 {
		query.Offset = 100000
	}
	return query
}

func appLogWhereClause(query AppLogQuery) (string, []any) {
	clauses := []string{"timestamp >= $1", "timestamp <= $2"}
	args := []any{query.From.UTC(), query.To.UTC()}
	addString := func(column, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	addString("level", strings.ToLower(query.Level))
	addString("log_kind", query.LogKind)
	addString("request_id", query.RequestID)
	addString("provider", query.Provider)
	addString("model", query.Model)
	addString("path", query.Path)
	addString("method", strings.ToUpper(query.Method))
	addString("source_file", query.SourceFile)
	if query.HTTPStatus != nil {
		args = append(args, *query.HTTPStatus)
		clauses = append(clauses, fmt.Sprintf("http_status = $%d", len(args)))
	}
	if query.StatusMin != nil {
		args = append(args, *query.StatusMin)
		clauses = append(clauses, fmt.Sprintf("http_status >= $%d", len(args)))
	}
	if query.StatusMax != nil {
		args = append(args, *query.StatusMax)
		clauses = append(clauses, fmt.Sprintf("http_status <= $%d", len(args)))
	}
	if text := strings.TrimSpace(query.Message); text != "" {
		args = append(args, "%"+text+"%")
		clauses = append(clauses, fmt.Sprintf("message ILIKE $%d", len(args)))
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}
