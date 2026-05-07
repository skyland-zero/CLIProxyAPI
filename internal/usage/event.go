package usage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/google/uuid"
	internallogging "github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func BuildEvent(ctx context.Context, record coreusage.Record) Event {
	timestamp := record.RequestedAt
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	tokens := normalizeTokens(TokenStats{
		InputTokens:     record.Detail.InputTokens,
		OutputTokens:    record.Detail.OutputTokens,
		ReasoningTokens: record.Detail.ReasoningTokens,
		CachedTokens:    record.Detail.CachedTokens,
		TotalTokens:     record.Detail.TotalTokens,
	})
	model := strings.TrimSpace(record.Model)
	if model == "" {
		model = "unknown"
	}
	alias := strings.TrimSpace(record.Alias)
	if alias == "" {
		alias = model
	}
	provider := strings.TrimSpace(record.Provider)
	if provider == "" {
		provider = "unknown"
	}
	authType := strings.TrimSpace(record.AuthType)
	if authType == "" {
		authType = "unknown"
	}
	failed := record.Failed
	if !failed {
		if status := internallogging.GetResponseStatus(ctx); status > 0 {
			failed = status >= 400
		}
	}
	latency := record.Latency.Milliseconds()
	if latency < 0 {
		latency = 0
	}
	return Event{
		ID:         uuid.NewString(),
		Timestamp:  timestamp.UTC(),
		RequestID:  strings.TrimSpace(internallogging.GetRequestID(ctx)),
		Endpoint:   strings.TrimSpace(internallogging.GetEndpoint(ctx)),
		Provider:   provider,
		Model:      model,
		Alias:      alias,
		AuthID:     strings.TrimSpace(record.AuthID),
		AuthIndex:  strings.TrimSpace(record.AuthIndex),
		AuthType:   authType,
		Source:     strings.TrimSpace(record.Source),
		APIKeyHash: HashAPIKey(record.APIKey),
		LatencyMs:  latency,
		Failed:     failed,
		Tokens:     tokens,
	}
}

func HashAPIKey(apiKey string) string {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}

func normalizeTokens(tokens TokenStats) TokenStats {
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens
	}
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens + tokens.CachedTokens
	}
	return tokens
}
