package management

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/pricing"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/redisqueue"
	usage "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

type usageQueueRecord []byte

func (r usageQueueRecord) MarshalJSON() ([]byte, error) {
	if json.Valid(r) {
		return append([]byte(nil), r...), nil
	}
	return json.Marshal(string(r))
}

// GetUsageQueue pops queued usage records from the usage queue.
func (h *Handler) GetUsageQueue(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler unavailable"})
		return
	}

	count, errCount := parseUsageQueueCount(c.Query("count"))
	if errCount != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errCount.Error()})
		return
	}

	items := redisqueue.PopOldest(count)
	records := make([]usageQueueRecord, 0, len(items))
	for _, item := range items {
		records = append(records, usageQueueRecord(append([]byte(nil), item...)))
	}

	c.JSON(http.StatusOK, records)
}

func parseUsageQueueCount(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 1, nil
	}
	count, errCount := strconv.Atoi(value)
	if errCount != nil || count <= 0 {
		return 0, errors.New("count must be a positive integer")
	}
	return count, nil
}

// GetUsageStatistics returns the current usage statistics snapshot.
func (h *Handler) GetUsageStatistics(c *gin.Context) {
	query, ok := parseUsageQuery(c)
	if !ok {
		return
	}
	snapshot, err := usage.Snapshot(c.Request.Context(), query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to query usage: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"mode":            usage.Mode(),
		"retention_days":  usage.RetentionDays,
		"usage":           snapshot,
		"failed_requests": snapshot.FailureCount,
	})
}

// GetUsageEvents returns non-destructive paginated usage events.
func (h *Handler) GetUsageEvents(c *gin.Context) {
	query, ok := parseUsageQuery(c)
	if !ok {
		return
	}
	events, total, err := usage.Events(c.Request.Context(), query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to query usage events: %v", err)})
		return
	}
	pricedEvents, err := pricing.PriceEvents(c.Request.Context(), events)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to estimate usage cost: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"mode":           usage.Mode(),
		"retention_days": usage.RetentionDays,
		"total":          total,
		"limit":          query.Limit,
		"offset":         query.Offset,
		"events":         pricedEvents,
	})
}

// GetUsageSummary returns grouped usage aggregates.
func (h *Handler) GetUsageSummary(c *gin.Context) {
	query, ok := parseUsageQuery(c)
	if !ok {
		return
	}
	groupBy := strings.TrimSpace(c.Query("group_by"))
	if groupBy == "" {
		groupBy = "provider"
	}
	tz := strings.TrimSpace(c.Query("tz"))
	if tz != "" {
		normalized, _, err := usage.ResolveSummaryTimeZone(tz)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid tz"})
			return
		}
		tz = normalized
	}
	summaryQuery := usage.SummaryQuery{
		Query:    query,
		GroupBy:  groupBy,
		TimeZone: tz,
	}
	var (
		rows []usage.SummaryRow
		err  error
	)
	if usage.Mode() == "memory" {
		eventQuery := query
		eventQuery.Limit = 1000
		eventQuery.Offset = 0
		events, _, errEvents := usage.Events(c.Request.Context(), eventQuery)
		if errEvents != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to query usage events: %v", errEvents)})
			return
		}
		rows, err = pricing.BuildPricedSummary(c.Request.Context(), events, summaryQuery)
	} else {
		rows, err = usage.Summary(c.Request.Context(), summaryQuery)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to query usage summary: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"mode":           usage.Mode(),
		"retention_days": usage.RetentionDays,
		"group_by":       groupBy,
		"tz":             tz,
		"summary":        rows,
	})
}

// DeleteUsageEvents deletes usage events matching the provided filters.
func (h *Handler) DeleteUsageEvents(c *gin.Context) {
	query, ok := parseUsageQuery(c)
	if !ok {
		return
	}
	if before := strings.TrimSpace(c.Query("before")); before != "" {
		parsed, err := parseUsageTime(before)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid before"})
			return
		}
		query.To = parsed
	}
	deleted, err := usage.Delete(c.Request.Context(), query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to delete usage events: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"mode":           usage.Mode(),
		"retention_days": usage.RetentionDays,
		"deleted":        deleted,
	})
}

func parseUsageQuery(c *gin.Context) (usage.Query, bool) {
	query := usage.Query{
		Provider: strings.TrimSpace(c.Query("provider")),
		Model:    strings.TrimSpace(c.Query("model")),
		Alias:    strings.TrimSpace(c.Query("alias")),
		AuthID:   strings.TrimSpace(c.Query("auth_id")),
		AuthType: strings.TrimSpace(c.Query("auth_type")),
		Source:   strings.TrimSpace(c.Query("source")),
		Limit:    100,
	}

	if value := strings.TrimSpace(c.Query("from")); value != "" {
		parsed, err := parseUsageTime(value)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from"})
			return usage.Query{}, false
		}
		query.From = parsed
	}
	if value := strings.TrimSpace(c.Query("to")); value != "" {
		parsed, err := parseUsageTime(value)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to"})
			return usage.Query{}, false
		}
		query.To = parsed
	}
	if value := strings.TrimSpace(c.Query("api_key_hash")); value != "" {
		query.APIKeyHash = value
	} else if value = strings.TrimSpace(c.Query("api_key")); value != "" {
		query.APIKeyHash = usage.HashAPIKey(value)
	}
	if value := strings.TrimSpace(c.Query("failed")); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid failed"})
			return usage.Query{}, false
		}
		query.Failed = &parsed
	}
	if value := strings.TrimSpace(c.Query("limit")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
			return usage.Query{}, false
		}
		query.Limit = parsed
	}
	if value := strings.TrimSpace(c.Query("offset")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "offset must be a non-negative integer"})
			return usage.Query{}, false
		}
		query.Offset = parsed
	}
	return query, true
}

func parseUsageTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC(), nil
	}
	unix, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(unix, 0).UTC(), nil
}
