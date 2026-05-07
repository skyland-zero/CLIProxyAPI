package usage

import (
	"context"
	"testing"
	"time"
)

func TestRecordQueuedPayloadWritesHashedEvent(t *testing.T) {
	previous := DefaultStore()
	store := NewMemoryStore()
	SetDefaultStore(store)
	t.Cleanup(func() { SetDefaultStore(previous) })

	payload := []byte(`{
		"timestamp":"2026-05-07T10:11:36.941224556Z",
		"latency_ms":4949,
		"source":"alice@example.com",
		"auth_index":"auth-index",
		"tokens":{"input_tokens":18141,"output_tokens":429,"reasoning_tokens":239,"cached_tokens":2432,"total_tokens":18570},
		"failed":false,
		"provider":"codex",
		"model":"gpt-5.4-mini",
		"alias":"gpt-5.4-mini",
		"endpoint":"POST /v1/responses",
		"auth_type":"oauth",
		"api_key":"client-key",
		"request_id":"request-id"
	}`)

	if err := RecordQueuedPayload(context.Background(), payload); err != nil {
		t.Fatalf("RecordQueuedPayload error: %v", err)
	}
	events, total, err := store.Events(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatalf("Events error: %v", err)
	}
	if total != 1 || len(events) != 1 {
		t.Fatalf("events total=%d len=%d, want 1", total, len(events))
	}
	event := events[0]
	if event.Provider != "codex" || event.Model != "gpt-5.4-mini" || event.Endpoint != "POST /v1/responses" {
		t.Fatalf("event fields = %+v", event)
	}
	if event.APIKeyHash == "" || event.APIKeyHash == "client-key" {
		t.Fatalf("api key hash = %q, want hashed value", event.APIKeyHash)
	}
	if event.Tokens.TotalTokens != 18570 || event.Tokens.CachedTokens != 2432 {
		t.Fatalf("tokens = %+v", event.Tokens)
	}
	if event.Timestamp.IsZero() || !event.Timestamp.Equal(time.Date(2026, 5, 7, 10, 11, 36, 941224556, time.UTC)) {
		t.Fatalf("timestamp = %s", event.Timestamp)
	}
}
