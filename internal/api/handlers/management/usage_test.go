package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/pricing"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/redisqueue"
	internalusage "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

func TestGetUsageQueuePopsRequestedRecords(t *testing.T) {
	gin.SetMode(gin.TestMode)
	withManagementUsageQueue(t, func() {
		redisqueue.Enqueue([]byte(`{"id":1}`))
		redisqueue.Enqueue([]byte(`{"id":2}`))
		redisqueue.Enqueue([]byte(`{"id":3}`))

		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage-queue?count=2", nil)

		h := &Handler{}
		h.GetUsageQueue(ginCtx)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload []json.RawMessage
		if errUnmarshal := json.Unmarshal(rec.Body.Bytes(), &payload); errUnmarshal != nil {
			t.Fatalf("unmarshal response: %v", errUnmarshal)
		}
		if len(payload) != 2 {
			t.Fatalf("response records = %d, want 2", len(payload))
		}
		requireRecordID(t, payload[0], 1)
		requireRecordID(t, payload[1], 2)

		remaining := redisqueue.PopOldest(10)
		if len(remaining) != 1 || string(remaining[0]) != `{"id":3}` {
			t.Fatalf("remaining queue = %q, want third item only", remaining)
		}
	})
}

func TestGetUsageQueueInvalidCountDoesNotPop(t *testing.T) {
	gin.SetMode(gin.TestMode)
	withManagementUsageQueue(t, func() {
		redisqueue.Enqueue([]byte(`{"id":1}`))

		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage-queue?count=0", nil)

		h := &Handler{}
		h.GetUsageQueue(ginCtx)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
		}

		remaining := redisqueue.PopOldest(10)
		if len(remaining) != 1 || string(remaining[0]) != `{"id":1}` {
			t.Fatalf("remaining queue = %q, want original item", remaining)
		}
	})
}

func TestGetUsageEventsFiltersByAPIKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	withMemoryPricingStore(t, func(priceStore *pricing.MemoryStore) {
		if err := priceStore.UpsertPrices(nil, []pricing.ModelPrice{{
			Provider:    pricing.ProviderOpenAI,
			Model:       "gpt",
			Category:    "standard",
			Context:     "short_context",
			Modality:    "text",
			Unit:        "1m_tokens",
			InputPer1M:  1,
			OutputPer1M: 2,
		}}); err != nil {
			t.Fatalf("upsert price: %v", err)
		}
		withMemoryUsageStore(t, func(store *internalusage.MemoryStore) {
			if err := store.Record(nil, internalusage.Event{
				ID:         "event-1",
				Timestamp:  time.Now().UTC(),
				Provider:   "openai",
				Model:      "gpt",
				APIKeyHash: internalusage.HashAPIKey("client-key"),
				Tokens:     internalusage.TokenStats{InputTokens: 1_000_000, OutputTokens: 1_000_000},
			}); err != nil {
				t.Fatalf("record usage: %v", err)
			}

			rec := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(rec)
			ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage/events?api_key=client-key&limit=10", nil)

			h := &Handler{}
			h.GetUsageEvents(ginCtx)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
			}
			var payload struct {
				Mode   string `json:"mode"`
				Total  int64  `json:"total"`
				Events []struct {
					internalusage.Event
					EstimatedCostUSD *float64 `json:"estimated_cost_usd"`
				} `json:"events"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if payload.Mode != "memory" || payload.Total != 1 || len(payload.Events) != 1 {
				t.Fatalf("payload = %+v, want one memory event", payload)
			}
			if payload.Events[0].APIKeyHash == "client-key" || payload.Events[0].APIKeyHash == "" {
				t.Fatalf("api key hash = %q, want hashed value only", payload.Events[0].APIKeyHash)
			}
			if payload.Events[0].EstimatedCostUSD == nil || *payload.Events[0].EstimatedCostUSD != 3 {
				t.Fatalf("estimated cost = %v, want 3", payload.Events[0].EstimatedCostUSD)
			}
		})
	})
}

func TestGetUsageSummary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	withMemoryUsageStore(t, func(store *internalusage.MemoryStore) {
		now := time.Now().UTC()
		_ = store.Record(nil, internalusage.Event{ID: "1", Timestamp: now, Provider: "openai", Model: "gpt", Tokens: internalusage.TokenStats{TotalTokens: 5}})
		_ = store.Record(nil, internalusage.Event{ID: "2", Timestamp: now, Provider: "openai", Model: "gpt", Failed: true, Tokens: internalusage.TokenStats{TotalTokens: 7}})

		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage/summary?group_by=provider", nil)

		h := &Handler{}
		h.GetUsageSummary(ginCtx)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var payload struct {
			Summary []internalusage.SummaryRow `json:"summary"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}
		if len(payload.Summary) != 1 || payload.Summary[0].Group != "openai" || payload.Summary[0].TotalRequests != 2 || payload.Summary[0].FailureCount != 1 {
			t.Fatalf("summary = %+v, want openai aggregate", payload.Summary)
		}
	})
}

func TestGetUsageSummaryDayRespectsTimeZoneAndCost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	withMemoryPricingStore(t, func(priceStore *pricing.MemoryStore) {
		if err := priceStore.UpsertPrices(nil, []pricing.ModelPrice{{
			Provider:    pricing.ProviderOpenAI,
			Model:       "gpt",
			Category:    "standard",
			Context:     "short_context",
			Modality:    "text",
			Unit:        "1m_tokens",
			InputPer1M:  1,
			OutputPer1M: 2,
		}}); err != nil {
			t.Fatalf("upsert price: %v", err)
		}
		withMemoryUsageStore(t, func(store *internalusage.MemoryStore) {
			first := time.Date(2026, 5, 6, 23, 30, 0, 0, time.UTC)
			second := time.Date(2026, 5, 7, 0, 30, 0, 0, time.UTC)
			_ = store.Record(nil, internalusage.Event{
				ID:        "1",
				Timestamp: first,
				Provider:  "openai",
				Model:     "gpt",
				Tokens: internalusage.TokenStats{
					InputTokens:  1_000_000,
					OutputTokens: 1_000_000,
					TotalTokens:  2_000_000,
				},
			})
			_ = store.Record(nil, internalusage.Event{
				ID:        "2",
				Timestamp: second,
				Provider:  "openai",
				Model:     "gpt",
				Tokens: internalusage.TokenStats{
					InputTokens:  1_000_000,
					OutputTokens: 1_000_000,
					TotalTokens:  2_000_000,
				},
			})

			rec := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(rec)
			ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage/summary?group_by=day&tz=Asia/Shanghai", nil)

			h := &Handler{}
			h.GetUsageSummary(ginCtx)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
			}
			var payload struct {
				TimeZone string                     `json:"tz"`
				Summary  []internalusage.SummaryRow `json:"summary"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if payload.TimeZone != "Asia/Shanghai" {
				t.Fatalf("tz = %q, want Asia/Shanghai", payload.TimeZone)
			}
			if len(payload.Summary) != 1 {
				t.Fatalf("summary rows = %d, want 1", len(payload.Summary))
			}
			row := payload.Summary[0]
			if row.Group != "2026-05-07" || row.TotalRequests != 2 || row.PricedRequests != 2 || row.UnpricedRequests != 0 {
				t.Fatalf("row = %+v, want one priced local-day bucket", row)
			}
			if row.EstimatedCostUSD == nil || *row.EstimatedCostUSD != 6 {
				t.Fatalf("estimated cost = %v, want 6", row.EstimatedCostUSD)
			}
		})
	})
}

func TestGetUsageSummaryInvalidTimeZone(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(rec)
	ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage/summary?group_by=day&tz=Nope/Invalid", nil)

	h := &Handler{}
	h.GetUsageSummary(ginCtx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func withManagementUsageQueue(t *testing.T, fn func()) {
	t.Helper()

	prevQueueEnabled := redisqueue.Enabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)

	defer func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
	}()

	fn()
}

func withMemoryUsageStore(t *testing.T, fn func(*internalusage.MemoryStore)) {
	t.Helper()
	previous := internalusage.DefaultStore()
	store := internalusage.NewMemoryStore()
	internalusage.SetDefaultStore(store)
	t.Cleanup(func() { internalusage.SetDefaultStore(previous) })
	fn(store)
}

func withMemoryPricingStore(t *testing.T, fn func(*pricing.MemoryStore)) {
	t.Helper()
	previous := pricing.DefaultStore()
	store := pricing.NewMemoryStore()
	pricing.SetDefaultStore(store)
	t.Cleanup(func() { pricing.SetDefaultStore(previous) })
	fn(store)
}

func requireRecordID(t *testing.T, raw json.RawMessage, want int) {
	t.Helper()

	var payload struct {
		ID int `json:"id"`
	}
	if errUnmarshal := json.Unmarshal(raw, &payload); errUnmarshal != nil {
		t.Fatalf("unmarshal record: %v", errUnmarshal)
	}
	if payload.ID != want {
		t.Fatalf("record id = %d, want %d", payload.ID, want)
	}
}
