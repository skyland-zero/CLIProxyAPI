package usage

import (
	"context"
	"time"
)

const (
	RetentionDays = 3
	retention     = RetentionDays * 24 * time.Hour
	defaultLimit  = 100
	maxLimit      = 1000
)

// TokenStats captures the token usage breakdown for a request.
type TokenStats struct {
	InputTokens     int64 `json:"input_tokens"`
	OutputTokens    int64 `json:"output_tokens"`
	ReasoningTokens int64 `json:"reasoning_tokens"`
	CachedTokens    int64 `json:"cached_tokens"`
	TotalTokens     int64 `json:"total_tokens"`
}

// Event is the normalized usage event persisted by the built-in usage tracker.
type Event struct {
	ID         string     `json:"id"`
	Timestamp  time.Time  `json:"timestamp"`
	RequestID  string     `json:"request_id"`
	Endpoint   string     `json:"endpoint"`
	Provider   string     `json:"provider"`
	Model      string     `json:"model"`
	Alias      string     `json:"alias"`
	AuthID     string     `json:"auth_id"`
	AuthIndex  string     `json:"auth_index"`
	AuthType   string     `json:"auth_type"`
	Source     string     `json:"source"`
	APIKeyHash string     `json:"api_key_hash"`
	LatencyMs  int64      `json:"latency_ms"`
	Failed     bool       `json:"failed"`
	Tokens     TokenStats `json:"tokens"`
}

// RequestDetail stores the per-request detail included in compatibility snapshots.
type RequestDetail struct {
	Timestamp  time.Time  `json:"timestamp"`
	LatencyMs  int64      `json:"latency_ms"`
	Source     string     `json:"source"`
	AuthIndex  string     `json:"auth_index"`
	AuthID     string     `json:"auth_id"`
	AuthType   string     `json:"auth_type"`
	RequestID  string     `json:"request_id"`
	Endpoint   string     `json:"endpoint"`
	APIKeyHash string     `json:"api_key_hash"`
	Tokens     TokenStats `json:"tokens"`
	Failed     bool       `json:"failed"`
}

// StatisticsSnapshot is an immutable view of aggregated usage metrics.
type StatisticsSnapshot struct {
	TotalRequests int64 `json:"total_requests"`
	SuccessCount  int64 `json:"success_count"`
	FailureCount  int64 `json:"failure_count"`
	TotalTokens   int64 `json:"total_tokens"`

	APIs map[string]APISnapshot `json:"apis"`

	RequestsByDay  map[string]int64 `json:"requests_by_day"`
	RequestsByHour map[string]int64 `json:"requests_by_hour"`
	TokensByDay    map[string]int64 `json:"tokens_by_day"`
	TokensByHour   map[string]int64 `json:"tokens_by_hour"`
}

// APISnapshot summarizes metrics for one client API key hash.
type APISnapshot struct {
	TotalRequests int64                    `json:"total_requests"`
	TotalTokens   int64                    `json:"total_tokens"`
	Models        map[string]ModelSnapshot `json:"models"`
}

// ModelSnapshot summarizes metrics for a specific model under one API key hash.
type ModelSnapshot struct {
	TotalRequests int64           `json:"total_requests"`
	TotalTokens   int64           `json:"total_tokens"`
	Details       []RequestDetail `json:"details"`
}

// Query filters usage events and snapshots.
type Query struct {
	From       time.Time
	To         time.Time
	Provider   string
	Model      string
	Alias      string
	AuthID     string
	AuthType   string
	Source     string
	APIKeyHash string
	Failed     *bool
	Limit      int
	Offset     int
}

// SummaryQuery filters and groups usage events.
type SummaryQuery struct {
	Query
	GroupBy string
}

// SummaryRow is one aggregate row returned by Summary.
type SummaryRow struct {
	Group           string `json:"group"`
	TotalRequests   int64  `json:"total_requests"`
	SuccessCount    int64  `json:"success_count"`
	FailureCount    int64  `json:"failure_count"`
	InputTokens     int64  `json:"input_tokens"`
	OutputTokens    int64  `json:"output_tokens"`
	ReasoningTokens int64  `json:"reasoning_tokens"`
	CachedTokens    int64  `json:"cached_tokens"`
	TotalTokens     int64  `json:"total_tokens"`
}

// Store records and queries usage events.
type Store interface {
	Record(ctx context.Context, event Event) error
	Snapshot(ctx context.Context, query Query) (StatisticsSnapshot, error)
	Events(ctx context.Context, query Query) ([]Event, int64, error)
	Summary(ctx context.Context, query SummaryQuery) ([]SummaryRow, error)
	Delete(ctx context.Context, query Query) (int64, error)
	Mode() string
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	if limit > maxLimit {
		return maxLimit
	}
	return limit
}

func retentionCutoff(now time.Time) time.Time {
	return now.Add(-retention)
}

func clampQueryToRetention(query Query, now time.Time) Query {
	cutoff := retentionCutoff(now)
	if query.From.IsZero() || query.From.Before(cutoff) {
		query.From = cutoff
	}
	if query.To.IsZero() || query.To.After(now) {
		query.To = now
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	query.Limit = normalizeLimit(query.Limit)
	return query
}
