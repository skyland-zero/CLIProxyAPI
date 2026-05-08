package management

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logdb"
)

func (h *Handler) GetAppLogs(c *gin.Context) {
	if !logdb.Enabled() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "postgres log store disabled"})
		return
	}
	query, ok := parseAppLogQuery(c)
	if !ok {
		return
	}
	records, total, err := logdb.QueryAppLogs(c.Request.Context(), query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to query app logs: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"mode":           "postgres",
		"retention_days": logdb.RetentionDays(),
		"total":          total,
		"limit":          query.Limit,
		"offset":         query.Offset,
		"logs":           records,
	})
}

func (h *Handler) GetRequestLogs(c *gin.Context) {
	if !logdb.Enabled() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "postgres log store disabled"})
		return
	}
	query, ok := parseRequestLogQuery(c)
	if !ok {
		return
	}
	records, total, err := logdb.QueryRequestLogs(c.Request.Context(), query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to query request logs: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"mode":           "postgres",
		"retention_days": logdb.RetentionDays(),
		"total":          total,
		"limit":          query.Limit,
		"offset":         query.Offset,
		"logs":           records,
	})
}

func (h *Handler) GetRequestLogRecord(c *gin.Context) {
	if !logdb.Enabled() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "postgres log store disabled"})
		return
	}
	logID := strings.TrimSpace(c.Param("id"))
	if logID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing log ID"})
		return
	}
	record, err := logdb.GetRequestLog(c.Request.Context(), logID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to query request log: %v", err)})
		return
	}
	if record == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "request log not found"})
		return
	}
	c.JSON(http.StatusOK, record)
}

func (h *Handler) GetRequestLogContent(c *gin.Context) {
	if !logdb.Enabled() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "postgres log store disabled"})
		return
	}
	logID := strings.TrimSpace(c.Param("id"))
	if logID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing log ID"})
		return
	}
	content, err := logdb.GetRequestLogContent(c.Request.Context(), logID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to query request log content: %v", err)})
		return
	}
	if content == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "request log content not found"})
		return
	}
	c.JSON(http.StatusOK, content)
}

func parseAppLogQuery(c *gin.Context) (logdb.AppLogQuery, bool) {
	query := logdb.AppLogQuery{
		Level:      strings.TrimSpace(c.Query("level")),
		LogKind:    strings.TrimSpace(c.Query("log_kind")),
		RequestID:  strings.TrimSpace(c.Query("request_id")),
		Provider:   strings.TrimSpace(c.Query("provider")),
		Model:      strings.TrimSpace(c.Query("model")),
		Path:       strings.TrimSpace(c.Query("path")),
		Method:     strings.TrimSpace(c.Query("method")),
		SourceFile: strings.TrimSpace(c.Query("source_file")),
		Message:    strings.TrimSpace(c.Query("message")),
		Limit:      100,
	}
	if strings.EqualFold(strings.TrimSpace(c.Query("include_total")), "true") {
		query.IncludeTotal = true
	}
	if value := strings.TrimSpace(c.Query("from")); value != "" {
		parsed, err := parseUsageTime(value)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from"})
			return logdb.AppLogQuery{}, false
		}
		query.From = parsed
	}
	if value := strings.TrimSpace(c.Query("to")); value != "" {
		parsed, err := parseUsageTime(value)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to"})
			return logdb.AppLogQuery{}, false
		}
		query.To = parsed
	}
	if parsed, ok := parseOptionalIntQuery(c, "http_status"); !ok {
		return logdb.AppLogQuery{}, false
	} else {
		query.HTTPStatus = parsed
	}
	if parsed, ok := parseOptionalIntQuery(c, "status_min"); !ok {
		return logdb.AppLogQuery{}, false
	} else {
		query.StatusMin = parsed
	}
	if parsed, ok := parseOptionalIntQuery(c, "status_max"); !ok {
		return logdb.AppLogQuery{}, false
	} else {
		query.StatusMax = parsed
	}
	if !parseLimitOffset(c, &query.Limit, &query.Offset) {
		return logdb.AppLogQuery{}, false
	}
	return query, true
}

func parseRequestLogQuery(c *gin.Context) (logdb.RequestLogQuery, bool) {
	query := logdb.RequestLogQuery{
		RequestID:           strings.TrimSpace(c.Query("request_id")),
		Endpoint:            strings.TrimSpace(c.Query("endpoint")),
		Method:              strings.TrimSpace(c.Query("method")),
		Provider:            strings.TrimSpace(c.Query("provider")),
		Model:               strings.TrimSpace(c.Query("model")),
		Alias:               strings.TrimSpace(c.Query("alias")),
		AuthID:              strings.TrimSpace(c.Query("auth_id")),
		AuthType:            strings.TrimSpace(c.Query("auth_type")),
		DownstreamTransport: strings.TrimSpace(c.Query("downstream_transport")),
		UpstreamTransport:   strings.TrimSpace(c.Query("upstream_transport")),
		Limit:               100,
	}
	if strings.EqualFold(strings.TrimSpace(c.Query("include_total")), "true") {
		query.IncludeTotal = true
	}
	if value := strings.TrimSpace(c.Query("from")); value != "" {
		parsed, err := parseUsageTime(value)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from"})
			return logdb.RequestLogQuery{}, false
		}
		query.From = parsed
	}
	if value := strings.TrimSpace(c.Query("to")); value != "" {
		parsed, err := parseUsageTime(value)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to"})
			return logdb.RequestLogQuery{}, false
		}
		query.To = parsed
	}
	if parsed, ok := parseOptionalIntQuery(c, "status_code"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.StatusCode = parsed
	}
	if parsed, ok := parseOptionalIntQuery(c, "status_min"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.StatusMin = parsed
	}
	if parsed, ok := parseOptionalIntQuery(c, "status_max"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.StatusMax = parsed
	}
	if parsed, ok := parseOptionalBoolQuery(c, "failed"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.Failed = parsed
	}
	if parsed, ok := parseOptionalBoolQuery(c, "has_api_error"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.HasAPIError = parsed
	}
	if parsed, ok := parseOptionalBoolQuery(c, "has_websocket_timeline"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.HasWebsocketTimeline = parsed
	}
	if parsed, ok := parseOptionalInt64Query(c, "request_bytes_min"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.RequestBytesMin = parsed
	}
	if parsed, ok := parseOptionalInt64Query(c, "request_bytes_max"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.RequestBytesMax = parsed
	}
	if parsed, ok := parseOptionalInt64Query(c, "response_bytes_min"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.ResponseBytesMin = parsed
	}
	if parsed, ok := parseOptionalInt64Query(c, "response_bytes_max"); !ok {
		return logdb.RequestLogQuery{}, false
	} else {
		query.ResponseBytesMax = parsed
	}
	if !parseLimitOffset(c, &query.Limit, &query.Offset) {
		return logdb.RequestLogQuery{}, false
	}
	return query, true
}

func parseLimitOffset(c *gin.Context, limit *int, offset *int) bool {
	if value := strings.TrimSpace(c.Query("limit")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
			return false
		}
		*limit = parsed
	}
	if value := strings.TrimSpace(c.Query("offset")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "offset must be a non-negative integer"})
			return false
		}
		*offset = parsed
	}
	return true
}

func parseOptionalIntQuery(c *gin.Context, key string) (*int, bool) {
	value := strings.TrimSpace(c.Query(key))
	if value == "" {
		return nil, true
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid %s", key)})
		return nil, false
	}
	return &parsed, true
}

func parseOptionalInt64Query(c *gin.Context, key string) (*int64, bool) {
	value := strings.TrimSpace(c.Query(key))
	if value == "" {
		return nil, true
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid %s", key)})
		return nil, false
	}
	return &parsed, true
}

func parseOptionalBoolQuery(c *gin.Context, key string) (*bool, bool) {
	value := strings.TrimSpace(c.Query(key))
	if value == "" {
		return nil, true
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid %s", key)})
		return nil, false
	}
	return &parsed, true
}
