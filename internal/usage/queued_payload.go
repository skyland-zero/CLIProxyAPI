package usage

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

type queuedPayload struct {
	Timestamp time.Time `json:"timestamp"`
	LatencyMs int64     `json:"latency_ms"`
	Source    string    `json:"source"`
	AuthIndex string    `json:"auth_index"`
	Tokens    struct {
		InputTokens     int64 `json:"input_tokens"`
		OutputTokens    int64 `json:"output_tokens"`
		ReasoningTokens int64 `json:"reasoning_tokens"`
		CachedTokens    int64 `json:"cached_tokens"`
		TotalTokens     int64 `json:"total_tokens"`
	} `json:"tokens"`
	Failed    bool   `json:"failed"`
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	Alias     string `json:"alias"`
	Endpoint  string `json:"endpoint"`
	AuthType  string `json:"auth_type"`
	APIKey    string `json:"api_key"`
	RequestID string `json:"request_id"`
}

// EventFromQueuedPayload converts the redisqueue usage payload into the durable usage event schema.
func EventFromQueuedPayload(payload []byte) (Event, error) {
	var queued queuedPayload
	if err := json.Unmarshal(payload, &queued); err != nil {
		return Event{}, err
	}
	now := nowUTC()
	timestamp := queued.Timestamp
	if timestamp.IsZero() {
		timestamp = now
	}
	model := strings.TrimSpace(queued.Model)
	if model == "" {
		model = "unknown"
	}
	alias := strings.TrimSpace(queued.Alias)
	if alias == "" {
		alias = model
	}
	provider := strings.TrimSpace(queued.Provider)
	if provider == "" {
		provider = "unknown"
	}
	authType := strings.TrimSpace(queued.AuthType)
	if authType == "" {
		authType = "unknown"
	}
	tokens := normalizeTokens(TokenStats{
		InputTokens:     queued.Tokens.InputTokens,
		OutputTokens:    queued.Tokens.OutputTokens,
		ReasoningTokens: queued.Tokens.ReasoningTokens,
		CachedTokens:    queued.Tokens.CachedTokens,
		TotalTokens:     queued.Tokens.TotalTokens,
	})
	latency := queued.LatencyMs
	if latency < 0 {
		latency = 0
	}
	return Event{
		ID:         uuid.NewString(),
		Timestamp:  timestamp.UTC(),
		RequestID:  strings.TrimSpace(queued.RequestID),
		Endpoint:   strings.TrimSpace(queued.Endpoint),
		Provider:   provider,
		Model:      model,
		Alias:      alias,
		AuthIndex:  strings.TrimSpace(queued.AuthIndex),
		AuthType:   authType,
		Source:     strings.TrimSpace(queued.Source),
		APIKeyHash: HashAPIKey(queued.APIKey),
		LatencyMs:  latency,
		Failed:     queued.Failed,
		Tokens:     tokens,
	}, nil
}

// RecordQueuedPayload writes a redisqueue usage payload to the active usage store.
func RecordQueuedPayload(ctx context.Context, payload []byte) error {
	if !StatisticsEnabled() {
		return nil
	}
	store := DefaultStore()
	if store == nil {
		return nil
	}
	event, err := EventFromQueuedPayload(payload)
	if err != nil {
		return err
	}
	return store.Record(ctx, event)
}
