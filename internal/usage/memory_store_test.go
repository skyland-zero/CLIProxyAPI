package usage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestHashAPIKeyUsesSHA256Hex(t *testing.T) {
	wantBytes := sha256.Sum256([]byte("client-key"))
	want := hex.EncodeToString(wantBytes[:])
	if got := HashAPIKey(" client-key "); got != want {
		t.Fatalf("HashAPIKey() = %q, want %q", got, want)
	}
	if got := HashAPIKey(" "); got != "" {
		t.Fatalf("HashAPIKey(empty) = %q, want empty", got)
	}
}

func TestMemoryStoreRecordsRecentEventsOnly(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now().UTC()
	recent := Event{ID: "recent", Timestamp: now.Add(-time.Hour), Provider: "openai", Model: "gpt", APIKeyHash: "hash", Tokens: TokenStats{InputTokens: 2, OutputTokens: 3}}
	old := Event{ID: "old", Timestamp: now.Add(-4 * 24 * time.Hour), Provider: "openai", Model: "gpt", APIKeyHash: "hash", Tokens: TokenStats{TotalTokens: 99}}

	if err := store.Record(context.Background(), old); err != nil {
		t.Fatalf("record old: %v", err)
	}
	if err := store.Record(context.Background(), recent); err != nil {
		t.Fatalf("record recent: %v", err)
	}

	events, total, err := store.Events(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatalf("Events error: %v", err)
	}
	if total != 1 || len(events) != 1 || events[0].ID != "recent" {
		t.Fatalf("events total=%d events=%v, want recent only", total, events)
	}
	snapshot, err := store.Snapshot(context.Background(), Query{})
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	if snapshot.TotalRequests != 1 || snapshot.TotalTokens != 5 {
		t.Fatalf("snapshot = %+v, want one request and normalized total 5", snapshot)
	}
}

func TestMemoryStoreSummaryGroupsByAPIKeyHash(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now().UTC()
	events := []Event{
		{ID: "1", Timestamp: now, Provider: "openai", Model: "gpt", APIKeyHash: "a", Tokens: TokenStats{TotalTokens: 10}},
		{ID: "2", Timestamp: now, Provider: "openai", Model: "gpt", APIKeyHash: "a", Failed: true, Tokens: TokenStats{TotalTokens: 5}},
		{ID: "3", Timestamp: now, Provider: "gemini", Model: "gemini", APIKeyHash: "b", Tokens: TokenStats{TotalTokens: 7}},
	}
	for _, event := range events {
		if err := store.Record(context.Background(), event); err != nil {
			t.Fatalf("Record error: %v", err)
		}
	}

	rows, err := store.Summary(context.Background(), SummaryQuery{GroupBy: "api_key_hash"})
	if err != nil {
		t.Fatalf("Summary error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("summary rows = %d, want 2: %+v", len(rows), rows)
	}
	if rows[0].Group != "a" || rows[0].TotalRequests != 2 || rows[0].FailureCount != 1 || rows[0].TotalTokens != 15 {
		t.Fatalf("first row = %+v, want aggregate for api key a", rows[0])
	}
}

func TestResolveSummaryTimeZoneSupportsAsiaShanghai(t *testing.T) {
	name, location, err := ResolveSummaryTimeZone("Asia/Shanghai")
	if err != nil {
		t.Fatalf("ResolveSummaryTimeZone error: %v", err)
	}
	if name != "Asia/Shanghai" {
		t.Fatalf("name = %q, want Asia/Shanghai", name)
	}
	if location == nil {
		t.Fatal("location = nil, want non-nil")
	}
}
