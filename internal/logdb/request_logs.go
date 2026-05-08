package logdb

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/tidwall/gjson"
)

const (
	maxRequestLogPayloadBytes   = 32 * 1024
	maxRequestLogContentBytes   = 32 * 1024
	maxRequestLogStreamingBytes = 4 * 1024 * 1024
)

type RequestLogEntry struct {
	ID                   string
	RequestID            string
	Timestamp            time.Time
	Endpoint             string
	Method               string
	Provider             string
	Model                string
	Alias                string
	AuthID               string
	AuthType             string
	StatusCode           int32
	Failed               bool
	HasAPIError          bool
	LatencyMs            int64
	DownstreamTransport  string
	UpstreamTransport    string
	RequestBytes         int64
	ResponseBytes        int64
	ContentBytes         int64
	HasWebsocketTimeline bool
}

type RequestLogContentRow struct {
	RequestLogID     string
	RequestID        string
	ContentGzip      []byte
	SizeUncompressed int64
	SchemaVersion    int32
}

type requestLogQueueItem struct {
	url                  string
	method               string
	requestHeaders       map[string][]string
	body                 []byte
	requestBytes         int64
	statusCode           int
	responseHeaders      map[string][]string
	response             []byte
	responseBytes        int64
	websocketTimeline    []byte
	apiRequest           []byte
	apiResponse          []byte
	apiWebsocketTimeline []byte
	apiResponseErrors    []*interfaces.ErrorMessage
	requestID            string
	requestTimestamp     time.Time
	apiResponseTimestamp time.Time
	truncationFlags      requestLogTruncationFlags
	queueBytes           int64
}

type RequestLogQuery struct {
	From                 time.Time
	To                   time.Time
	RequestID            string
	Endpoint             string
	Method               string
	Provider             string
	Model                string
	Alias                string
	AuthID               string
	AuthType             string
	StatusCode           *int
	StatusMin            *int
	StatusMax            *int
	Failed               *bool
	HasAPIError          *bool
	DownstreamTransport  string
	UpstreamTransport    string
	HasWebsocketTimeline *bool
	RequestBytesMin      *int64
	RequestBytesMax      *int64
	ResponseBytesMin     *int64
	ResponseBytesMax     *int64
	Limit                int
	Offset               int
	IncludeTotal         bool
}

type RequestLogRecord struct {
	ID                   string    `json:"id"`
	RequestID            string    `json:"request_id"`
	Timestamp            time.Time `json:"timestamp"`
	Endpoint             string    `json:"endpoint"`
	Method               string    `json:"method"`
	Provider             string    `json:"provider"`
	Model                string    `json:"model"`
	Alias                string    `json:"alias"`
	AuthID               string    `json:"auth_id"`
	AuthType             string    `json:"auth_type"`
	StatusCode           int       `json:"status_code"`
	Failed               bool      `json:"failed"`
	HasAPIError          bool      `json:"has_api_error"`
	LatencyMs            int64     `json:"latency_ms"`
	DownstreamTransport  string    `json:"downstream_transport"`
	UpstreamTransport    string    `json:"upstream_transport"`
	RequestBytes         int64     `json:"request_bytes"`
	ResponseBytes        int64     `json:"response_bytes"`
	ContentBytes         int64     `json:"content_bytes"`
	HasWebsocketTimeline bool      `json:"has_websocket_timeline"`
}

type RequestLogContent struct {
	RequestID  string                `json:"request_id"`
	Timestamp  time.Time             `json:"timestamp"`
	Request    requestContentSection `json:"request"`
	Downstream downstreamContent     `json:"downstream"`
	Upstream   upstreamContent       `json:"upstream"`
	Meta       requestContentMeta    `json:"meta"`
	Raw        requestContentRaw     `json:"raw"`
}

type requestContentSection struct {
	URL     string              `json:"url"`
	Method  string              `json:"method"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
}

type downstreamContent struct {
	Transport         string              `json:"transport"`
	StatusCode        int                 `json:"status_code"`
	Headers           map[string][]string `json:"headers"`
	Body              string              `json:"body"`
	WebsocketTimeline string              `json:"websocket_timeline"`
}

type upstreamError struct {
	StatusCode int    `json:"status_code"`
	Message    string `json:"message"`
}

type upstreamContent struct {
	Transport         string          `json:"transport"`
	APIRequest        string          `json:"api_request"`
	APIResponse       string          `json:"api_response"`
	APIWebsocketTrace string          `json:"api_websocket_timeline"`
	APIErrors         []upstreamError `json:"api_errors"`
}

type requestContentMeta struct {
	Provider              string `json:"provider"`
	Model                 string `json:"model"`
	Alias                 string `json:"alias"`
	AuthID                string `json:"auth_id"`
	AuthType              string `json:"auth_type"`
	LatencyMs             int64  `json:"latency_ms"`
	Failed                bool   `json:"failed"`
	HasAPIError           bool   `json:"has_api_error"`
	DownstreamTransport   string `json:"downstream_transport"`
	UpstreamTransport     string `json:"upstream_transport"`
	HasWebsocketTimeline  bool   `json:"has_websocket_timeline"`
	RequestBodyTruncated  bool   `json:"request_body_truncated"`
	ResponseBodyTruncated bool   `json:"response_body_truncated"`
	APIRequestTruncated   bool   `json:"api_request_truncated"`
	APIResponseTruncated  bool   `json:"api_response_truncated"`
	APITimelineTruncated  bool   `json:"api_timeline_truncated"`
}

type requestContentRaw struct {
	RequestBytes  int64 `json:"request_bytes"`
	ResponseBytes int64 `json:"response_bytes"`
	ContentBytes  int64 `json:"content_bytes"`
}

type PostgresRequestLogger struct {
	enabled atomic.Bool
}

type requestLogTruncationFlags struct {
	requestBody  bool
	responseBody bool
	apiRequest   bool
	apiResponse  bool
	apiTimeline  bool
}

var authProviderPattern = regexp.MustCompile(`provider=([^,\s]+)`)
var authIDPattern = regexp.MustCompile(`auth_id=([^,\s]+)`)
var authTypePattern = regexp.MustCompile(`type=([^,\s]+)`)
var upstreamURLPattern = regexp.MustCompile(`Upstream URL:\s*(.+)`)

func NewRequestLogger(enabled bool) *PostgresRequestLogger {
	logger := &PostgresRequestLogger{}
	logger.enabled.Store(enabled)
	return logger
}

func (l *PostgresRequestLogger) IsEnabled() bool {
	return l != nil && l.enabled.Load()
}

func (l *PostgresRequestLogger) SetEnabled(enabled bool) {
	if l != nil {
		l.enabled.Store(enabled)
	}
}

func (l *PostgresRequestLogger) LogRequest(url, method string, requestHeaders map[string][]string, body []byte, statusCode int, responseHeaders map[string][]string, response, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline []byte, apiResponseErrors []*interfaces.ErrorMessage, requestID string, requestTimestamp, apiResponseTimestamp time.Time) error {
	return l.logRequest(url, method, requestHeaders, body, int64(len(body)), statusCode, responseHeaders, response, int64(len(response)), websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline, apiResponseErrors, requestID, requestTimestamp, apiResponseTimestamp, requestLogTruncationFlags{}, false)
}

func (l *PostgresRequestLogger) LogRequestWithOptions(url, method string, requestHeaders map[string][]string, body []byte, statusCode int, responseHeaders map[string][]string, response, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline []byte, apiResponseErrors []*interfaces.ErrorMessage, force bool, requestID string, requestTimestamp, apiResponseTimestamp time.Time) error {
	return l.logRequest(url, method, requestHeaders, body, int64(len(body)), statusCode, responseHeaders, response, int64(len(response)), websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline, apiResponseErrors, requestID, requestTimestamp, apiResponseTimestamp, requestLogTruncationFlags{}, force)
}

func (l *PostgresRequestLogger) LogRequestWithCaptureInfo(url, method string, requestHeaders map[string][]string, body []byte, requestBodyTruncated bool, requestBytes int64, statusCode int, responseHeaders map[string][]string, response []byte, responseBodyTruncated bool, responseBytes int64, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline []byte, apiResponseErrors []*interfaces.ErrorMessage, force bool, requestID string, requestTimestamp, apiResponseTimestamp time.Time) error {
	return l.logRequest(url, method, requestHeaders, body, requestBytes, statusCode, responseHeaders, response, responseBytes, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline, apiResponseErrors, requestID, requestTimestamp, apiResponseTimestamp, requestLogTruncationFlags{requestBody: requestBodyTruncated, responseBody: responseBodyTruncated}, force)
}

func (l *PostgresRequestLogger) logRequest(url, method string, requestHeaders map[string][]string, body []byte, requestBytes int64, statusCode int, responseHeaders map[string][]string, response []byte, responseBytes int64, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline []byte, apiResponseErrors []*interfaces.ErrorMessage, requestID string, requestTimestamp, apiResponseTimestamp time.Time, truncationFlags requestLogTruncationFlags, force bool) error {
	if !l.IsEnabled() && !force {
		return nil
	}
	mgr := DefaultManager()
	if mgr == nil || mgr.closed.Load() {
		return nil
	}
	queueBytes := estimateRequestLogQueueBytes(body, response, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline)
	if !mgr.reserveRequestLogSlot(queueBytes) {
		return nil
	}
	requestBody, requestBodyTruncated := cloneTruncated(body, maxRequestLogPayloadBytes)
	responseBody, responseBodyTruncated := cloneTruncated(response, maxRequestLogStreamingBytes)
	websocketTimelineBody, websocketTimelineTruncated := cloneTruncated(websocketTimeline, maxRequestLogContentBytes)
	apiRequestBody, apiRequestTruncated := cloneTruncated(apiRequest, maxRequestLogPayloadBytes)
	apiResponseBody, apiResponseTruncated := cloneTruncated(apiResponse, maxRequestLogPayloadBytes)
	apiTimelineBody, apiTimelineTruncated := cloneTruncated(apiWebsocketTimeline, maxRequestLogPayloadBytes)
	truncationFlags.requestBody = truncationFlags.requestBody || requestBodyTruncated
	truncationFlags.responseBody = truncationFlags.responseBody || responseBodyTruncated
	truncationFlags.apiRequest = truncationFlags.apiRequest || apiRequestTruncated
	truncationFlags.apiResponse = truncationFlags.apiResponse || apiResponseTruncated
	truncationFlags.apiTimeline = truncationFlags.apiTimeline || apiTimelineTruncated || websocketTimelineTruncated
	item := requestLogQueueItem{
		url:                  url,
		method:               method,
		requestHeaders:       cloneHeaders(requestHeaders),
		body:                 requestBody,
		requestBytes:         requestBytes,
		statusCode:           statusCode,
		responseHeaders:      cloneHeaders(responseHeaders),
		response:             responseBody,
		responseBytes:        responseBytes,
		websocketTimeline:    websocketTimelineBody,
		apiRequest:           apiRequestBody,
		apiResponse:          apiResponseBody,
		apiWebsocketTimeline: apiTimelineBody,
		apiResponseErrors:    append([]*interfaces.ErrorMessage(nil), apiResponseErrors...),
		requestID:            requestID,
		requestTimestamp:     requestTimestamp,
		apiResponseTimestamp: apiResponseTimestamp,
		truncationFlags:      truncationFlags,
		queueBytes:           queueBytes,
	}
	mgr.enqueueMu.RLock()
	defer mgr.enqueueMu.RUnlock()
	if mgr.closed.Load() {
		mgr.releaseRequestLogSlot(queueBytes)
		return nil
	}
	select {
	case mgr.requestLogCh <- item:
	default:
		mgr.releaseRequestLogSlot(queueBytes)
	}
	return nil
}

func (l *PostgresRequestLogger) LogStreamingRequest(url, method string, headers map[string][]string, body []byte, requestID string) (logging.StreamingLogWriter, error) {
	return l.LogStreamingRequestWithCaptureInfo(url, method, headers, body, false, int64(len(body)), requestID)
}

func (l *PostgresRequestLogger) LogStreamingRequestWithCaptureInfo(url, method string, headers map[string][]string, body []byte, requestBodyTruncated bool, requestBytes int64, requestID string) (logging.StreamingLogWriter, error) {
	if !l.IsEnabled() {
		return &logging.NoOpStreamingLogWriter{}, nil
	}
	mgr := DefaultManager()
	if mgr == nil || mgr.closed.Load() {
		return &logging.NoOpStreamingLogWriter{}, nil
	}
	requestHeaders := cloneHeaders(headers)
	requestBody, requestBodyWasTruncated := cloneTruncated(body, maxRequestLogPayloadBytes)
	requestBodyTruncated = requestBodyTruncated || requestBodyWasTruncated
	if requestBytes < 0 {
		requestBytes = int64(len(body))
	}
	return &PostgresStreamingLogWriter{logger: l, mgr: mgr, url: url, method: method, headers: requestHeaders, body: requestBody, requestBytes: requestBytes, requestID: requestID, timestamp: time.Now().UTC(), truncationFlags: requestLogTruncationFlags{requestBody: requestBodyTruncated}}, nil
}

type PostgresStreamingLogWriter struct {
	logger               *PostgresRequestLogger
	mgr                  *Manager
	url                  string
	method               string
	headers              map[string][]string
	body                 []byte
	requestBytes         int64
	requestID            string
	timestamp            time.Time
	firstChunkTimestamp  time.Time
	responseStatus       int
	responseHeaders      map[string][]string
	apiRequest           []byte
	apiResponse          []byte
	apiWebsocketTimeline []byte
	responseBody         bytes.Buffer
	responseBytes        int64
	streamBytesReserved  int64
	truncationFlags      requestLogTruncationFlags
}

func (w *PostgresStreamingLogWriter) WriteChunkAsync(chunk []byte) {
	w.responseBytes += int64(len(chunk))
	if w.responseBody.Len() >= maxRequestLogStreamingBytes {
		w.truncationFlags.responseBody = true
		return
	}
	remaining := maxRequestLogStreamingBytes - w.responseBody.Len()
	if len(chunk) > remaining {
		if !w.reserveStreamBytes(int64(remaining)) {
			w.truncationFlags.responseBody = true
			return
		}
		_, _ = w.responseBody.Write(chunk[:remaining])
		w.truncationFlags.responseBody = true
		return
	}
	if !w.reserveStreamBytes(int64(len(chunk))) {
		w.truncationFlags.responseBody = true
		return
	}
	_, _ = w.responseBody.Write(chunk)
}

func (w *PostgresStreamingLogWriter) reserveStreamBytes(bytes int64) bool {
	if bytes <= 0 {
		return true
	}
	if w.mgr == nil || w.mgr.closed.Load() {
		return false
	}
	newBytes := w.mgr.requestLogBytes.Add(bytes)
	if w.mgr.maxRequestLogQueueBytes > 0 && newBytes > w.mgr.maxRequestLogQueueBytes {
		w.mgr.requestLogBytes.Add(-bytes)
		return false
	}
	w.streamBytesReserved += bytes
	return true
}

func (w *PostgresStreamingLogWriter) releaseStreamBytes() {
	if w.mgr == nil || w.streamBytesReserved <= 0 {
		return
	}
	w.mgr.requestLogBytes.Add(-w.streamBytesReserved)
	w.streamBytesReserved = 0
}

func (w *PostgresStreamingLogWriter) MarkResponseTruncated() {
	w.truncationFlags.responseBody = true
}

func (w *PostgresStreamingLogWriter) WriteStatus(status int, headers map[string][]string) error {
	w.responseStatus = status
	w.responseHeaders = cloneHeaders(headers)
	return nil
}

func (w *PostgresStreamingLogWriter) WriteAPIRequest(apiRequest []byte) error {
	w.apiRequest, w.truncationFlags.apiRequest = cloneTruncated(apiRequest, maxRequestLogPayloadBytes)
	return nil
}

func (w *PostgresStreamingLogWriter) WriteAPIResponse(apiResponse []byte) error {
	w.apiResponse, w.truncationFlags.apiResponse = cloneTruncated(apiResponse, maxRequestLogPayloadBytes)
	return nil
}

func (w *PostgresStreamingLogWriter) WriteAPIWebsocketTimeline(apiWebsocketTimeline []byte) error {
	w.apiWebsocketTimeline, w.truncationFlags.apiTimeline = cloneTruncated(apiWebsocketTimeline, maxRequestLogPayloadBytes)
	return nil
}

func (w *PostgresStreamingLogWriter) SetFirstChunkTimestamp(timestamp time.Time) {
	if !timestamp.IsZero() {
		w.firstChunkTimestamp = timestamp.UTC()
	}
}

func (w *PostgresStreamingLogWriter) Close() error {
	if w.logger == nil {
		return nil
	}
	apiResponseTimestamp := w.firstChunkTimestamp
	if apiResponseTimestamp.IsZero() {
		apiResponseTimestamp = time.Now().UTC()
	}
	w.releaseStreamBytes()
	return w.logger.logRequest(w.url, w.method, w.headers, w.body, w.requestBytes, w.responseStatus, w.responseHeaders, w.responseBody.Bytes(), w.responseBytes, nil, w.apiRequest, w.apiResponse, w.apiWebsocketTimeline, nil, w.requestID, w.timestamp, apiResponseTimestamp, w.truncationFlags, false)
}

func (m *Manager) reserveRequestLogSlot(queueBytes int64) bool {
	if m == nil || m.closed.Load() {
		return false
	}
	if queueBytes < 0 {
		queueBytes = 0
	}
	select {
	case m.requestLogReserve <- struct{}{}:
		newBytes := m.requestLogBytes.Add(queueBytes)
		if m.maxRequestLogQueueBytes > 0 && newBytes > m.maxRequestLogQueueBytes {
			m.requestLogBytes.Add(-queueBytes)
			<-m.requestLogReserve
			return false
		}
		return true
	default:
		return false
	}
}

func (m *Manager) releaseRequestLogSlot(queueBytes int64) {
	if m == nil {
		return
	}
	if queueBytes > 0 {
		m.requestLogBytes.Add(-queueBytes)
	}
	select {
	case <-m.requestLogReserve:
	default:
	}
}

func estimateRequestLogQueueBytes(body, response, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline []byte) int64 {
	const perRecordOverhead = 8 * 1024
	return int64(perRecordOverhead + cappedLen(body, maxRequestLogPayloadBytes) + cappedLen(response, maxRequestLogStreamingBytes) + cappedLen(websocketTimeline, maxRequestLogContentBytes) + cappedLen(apiRequest, maxRequestLogPayloadBytes) + cappedLen(apiResponse, maxRequestLogPayloadBytes) + cappedLen(apiWebsocketTimeline, maxRequestLogPayloadBytes))
}

func cappedLen(payload []byte, maxBytes int) int {
	if len(payload) > maxBytes {
		return maxBytes
	}
	return len(payload)
}

func buildRequestLogRecord(url, method string, requestHeaders map[string][]string, body []byte, requestBytes int64, statusCode int, responseHeaders map[string][]string, response []byte, responseBytes int64, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline []byte, apiResponseErrors []*interfaces.ErrorMessage, requestID string, requestTimestamp, apiResponseTimestamp time.Time, truncationFlags requestLogTruncationFlags) ([]byte, RequestLogEntry, RequestLogContentRow, error) {
	if requestTimestamp.IsZero() {
		requestTimestamp = time.Now().UTC()
	}
	if requestBytes == 0 && len(body) > 0 {
		requestBytes = int64(len(body))
	}
	if responseBytes == 0 && len(response) > 0 {
		responseBytes = int64(len(response))
	}
	requestID = strings.TrimSpace(requestID)
	entryID := uuid.NewString()
	if requestID == "" {
		requestID = entryID
	}
	endpoint := stripQuery(url)
	provider, authID, authType := extractAuthMetadata(apiRequest)
	model := extractModel(body, apiRequest)
	if model == "" {
		model = extractModelFromURL(url)
	}
	alias := model
	downstreamTransport := inferDownstreamTransport(requestHeaders, websocketTimeline)
	upstreamTransport := inferUpstreamTransport(apiRequest, apiResponse, apiWebsocketTimeline)
	hasAPIError := len(apiResponseErrors) > 0 || statusCode >= 400
	latencyMs := int64(0)
	if !apiResponseTimestamp.IsZero() && !requestTimestamp.IsZero() {
		latencyMs = apiResponseTimestamp.Sub(requestTimestamp).Milliseconds()
		if latencyMs < 0 {
			latencyMs = 0
		}
	}
	requestBody, requestBodyTruncated := truncatePayload(body, maxRequestLogPayloadBytes)
	responseBody, responseBodyTruncated := truncatePayload(response, maxRequestLogStreamingBytes)
	apiRequestBody, apiRequestTruncated := truncatePayload(apiRequest, maxRequestLogPayloadBytes)
	apiResponseBody, apiResponseTruncated := truncatePayload(apiResponse, maxRequestLogPayloadBytes)
	apiTimelineBody, apiTimelineTruncated := truncatePayload(apiWebsocketTimeline, maxRequestLogPayloadBytes)
	websocketTimelineBody, websocketTimelineTruncated := truncatePayload(websocketTimeline, maxRequestLogContentBytes)
	content := RequestLogContent{
		RequestID: requestID,
		Timestamp: requestTimestamp.UTC(),
		Request: requestContentSection{
			URL:     url,
			Method:  method,
			Headers: cloneHeaders(requestHeaders),
			Body:    string(requestBody),
		},
		Downstream: downstreamContent{
			Transport:         downstreamTransport,
			StatusCode:        statusCode,
			Headers:           cloneHeaders(responseHeaders),
			Body:              string(responseBody),
			WebsocketTimeline: string(websocketTimelineBody),
		},
		Upstream: upstreamContent{
			Transport:         upstreamTransport,
			APIRequest:        string(apiRequestBody),
			APIResponse:       string(apiResponseBody),
			APIWebsocketTrace: string(apiTimelineBody),
			APIErrors:         flattenAPIErrors(apiResponseErrors),
		},
		Meta: requestContentMeta{
			Provider:              provider,
			Model:                 model,
			Alias:                 alias,
			AuthID:                authID,
			AuthType:              authType,
			LatencyMs:             latencyMs,
			Failed:                statusCode >= 400,
			HasAPIError:           hasAPIError,
			DownstreamTransport:   downstreamTransport,
			UpstreamTransport:     upstreamTransport,
			HasWebsocketTimeline:  hasPayload(websocketTimeline),
			RequestBodyTruncated:  truncationFlags.requestBody || requestBodyTruncated,
			ResponseBodyTruncated: truncationFlags.responseBody || responseBodyTruncated,
			APIRequestTruncated:   truncationFlags.apiRequest || apiRequestTruncated,
			APIResponseTruncated:  truncationFlags.apiResponse || apiResponseTruncated,
			APITimelineTruncated:  truncationFlags.apiTimeline || apiTimelineTruncated || websocketTimelineTruncated,
		},
		Raw: requestContentRaw{
			RequestBytes:  requestBytes,
			ResponseBytes: responseBytes,
			ContentBytes:  0,
		},
	}
	content.Raw.ContentBytes = int64(len(requestBody) + len(responseBody) + len(apiRequestBody) + len(apiResponseBody) + len(apiTimelineBody) + len(websocketTimelineBody))
	contentBytes, err := json.Marshal(content)
	if err != nil {
		return nil, RequestLogEntry{}, RequestLogContentRow{}, err
	}
	compressedContent, err := gzipBytes(contentBytes)
	if err != nil {
		return nil, RequestLogEntry{}, RequestLogContentRow{}, err
	}
	entry := RequestLogEntry{
		ID:                   entryID,
		RequestID:            requestID,
		Timestamp:            requestTimestamp.UTC(),
		Endpoint:             endpoint,
		Method:               strings.ToUpper(strings.TrimSpace(method)),
		Provider:             provider,
		Model:                model,
		Alias:                alias,
		AuthID:               authID,
		AuthType:             authType,
		StatusCode:           int32(statusCode),
		Failed:               statusCode >= 400,
		HasAPIError:          hasAPIError,
		LatencyMs:            latencyMs,
		DownstreamTransport:  downstreamTransport,
		UpstreamTransport:    upstreamTransport,
		RequestBytes:         requestBytes,
		ResponseBytes:        responseBytes,
		HasWebsocketTimeline: hasPayload(websocketTimeline),
	}
	entry.ContentBytes = int64(len(contentBytes))
	contentRow := RequestLogContentRow{RequestLogID: entryID, RequestID: requestID, ContentGzip: compressedContent, SizeUncompressed: int64(len(contentBytes)), SchemaVersion: 1}
	return contentBytes, entry, contentRow, nil
}

func truncatePayload(payload []byte, maxBytes int) ([]byte, bool) {
	if len(payload) <= maxBytes {
		return payload, false
	}
	return payload[:maxBytes], true
}

func cloneTruncated(payload []byte, maxBytes int) ([]byte, bool) {
	truncatedPayload, truncated := truncatePayload(payload, maxBytes)
	return bytes.Clone(truncatedPayload), truncated
}

func QueryRequestLogs(ctx context.Context, query RequestLogQuery) ([]RequestLogRecord, int64, error) {
	mgr := DefaultManager()
	if mgr == nil {
		return nil, 0, nil
	}
	return mgr.queryRequestLogs(ctx, query)
}

func GetRequestLog(ctx context.Context, logID string) (*RequestLogRecord, error) {
	mgr := DefaultManager()
	if mgr == nil {
		return nil, nil
	}
	return mgr.getRequestLog(ctx, logID)
}

func GetRequestLogContent(ctx context.Context, logID string) (*RequestLogContent, error) {
	mgr := DefaultManager()
	if mgr == nil {
		return nil, nil
	}
	return mgr.getRequestLogContent(ctx, logID)
}

func (m *Manager) queryRequestLogs(ctx context.Context, query RequestLogQuery) ([]RequestLogRecord, int64, error) {
	query = normalizeRequestLogQuery(query)
	where, args := requestLogWhereClause(query)
	total := int64(-1)
	if query.IncludeTotal {
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", m.schemaTable(requestLogTable), where)
		if err := m.pool.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
			return nil, 0, err
		}
	}
	selectArgs := append([]any(nil), args...)
	selectArgs = append(selectArgs, query.Limit, query.Offset)
	querySQL := fmt.Sprintf(`
		SELECT id, request_id, timestamp, endpoint, method, provider, model, alias, auth_id, auth_type, status_code, failed, has_api_error, latency_ms, downstream_transport, upstream_transport, request_bytes, response_bytes, content_bytes, has_websocket_timeline
		FROM %s %s
		ORDER BY timestamp DESC, id DESC
		LIMIT $%d OFFSET $%d
	`, m.schemaTable(requestLogTable), where, len(selectArgs)-1, len(selectArgs))
	rows, err := m.pool.Query(ctx, querySQL, selectArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := make([]RequestLogRecord, 0, query.Limit)
	for rows.Next() {
		var record RequestLogRecord
		if err = rows.Scan(&record.ID, &record.RequestID, &record.Timestamp, &record.Endpoint, &record.Method, &record.Provider, &record.Model, &record.Alias, &record.AuthID, &record.AuthType, &record.StatusCode, &record.Failed, &record.HasAPIError, &record.LatencyMs, &record.DownstreamTransport, &record.UpstreamTransport, &record.RequestBytes, &record.ResponseBytes, &record.ContentBytes, &record.HasWebsocketTimeline); err != nil {
			return nil, 0, err
		}
		record.Timestamp = record.Timestamp.UTC()
		out = append(out, record)
	}
	return out, total, rows.Err()
}

func (m *Manager) getRequestLog(ctx context.Context, logID string) (*RequestLogRecord, error) {
	logID = strings.TrimSpace(logID)
	if logID == "" {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT id, request_id, timestamp, endpoint, method, provider, model, alias, auth_id, auth_type, status_code, failed, has_api_error, latency_ms, downstream_transport, upstream_transport, request_bytes, response_bytes, content_bytes, has_websocket_timeline
		FROM %s WHERE id = $1
		LIMIT 1
	`, m.schemaTable(requestLogTable))
	var record RequestLogRecord
	if err := m.pool.QueryRow(ctx, query, logID).Scan(&record.ID, &record.RequestID, &record.Timestamp, &record.Endpoint, &record.Method, &record.Provider, &record.Model, &record.Alias, &record.AuthID, &record.AuthType, &record.StatusCode, &record.Failed, &record.HasAPIError, &record.LatencyMs, &record.DownstreamTransport, &record.UpstreamTransport, &record.RequestBytes, &record.ResponseBytes, &record.ContentBytes, &record.HasWebsocketTimeline); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	record.Timestamp = record.Timestamp.UTC()
	return &record, nil
}

func (m *Manager) getRequestLogContent(ctx context.Context, logID string) (*RequestLogContent, error) {
	logID = strings.TrimSpace(logID)
	if logID == "" {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT content_gzip
		FROM %s WHERE request_log_id = $1
		LIMIT 1
	`, m.schemaTable(requestContentTable))
	var payload []byte
	if err := m.pool.QueryRow(ctx, query, logID).Scan(&payload); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	decoded, err := gunzipBytes(payload)
	if err != nil {
		return nil, err
	}
	var content RequestLogContent
	if err = json.Unmarshal(decoded, &content); err != nil {
		return nil, err
	}
	content.Timestamp = content.Timestamp.UTC()
	return &content, nil
}

func normalizeRequestLogQuery(query RequestLogQuery) RequestLogQuery {
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

func requestLogWhereClause(query RequestLogQuery) (string, []any) {
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
	addString("request_id", query.RequestID)
	addString("endpoint", query.Endpoint)
	addString("method", strings.ToUpper(query.Method))
	addString("provider", query.Provider)
	addString("model", query.Model)
	addString("alias", query.Alias)
	addString("auth_id", query.AuthID)
	addString("auth_type", query.AuthType)
	addString("downstream_transport", query.DownstreamTransport)
	addString("upstream_transport", query.UpstreamTransport)
	if query.StatusCode != nil {
		args = append(args, *query.StatusCode)
		clauses = append(clauses, fmt.Sprintf("status_code = $%d", len(args)))
	}
	if query.StatusMin != nil {
		args = append(args, *query.StatusMin)
		clauses = append(clauses, fmt.Sprintf("status_code >= $%d", len(args)))
	}
	if query.StatusMax != nil {
		args = append(args, *query.StatusMax)
		clauses = append(clauses, fmt.Sprintf("status_code <= $%d", len(args)))
	}
	if query.Failed != nil {
		args = append(args, *query.Failed)
		clauses = append(clauses, fmt.Sprintf("failed = $%d", len(args)))
	}
	if query.HasAPIError != nil {
		args = append(args, *query.HasAPIError)
		clauses = append(clauses, fmt.Sprintf("has_api_error = $%d", len(args)))
	}
	if query.HasWebsocketTimeline != nil {
		args = append(args, *query.HasWebsocketTimeline)
		clauses = append(clauses, fmt.Sprintf("has_websocket_timeline = $%d", len(args)))
	}
	if query.RequestBytesMin != nil {
		args = append(args, *query.RequestBytesMin)
		clauses = append(clauses, fmt.Sprintf("request_bytes >= $%d", len(args)))
	}
	if query.RequestBytesMax != nil {
		args = append(args, *query.RequestBytesMax)
		clauses = append(clauses, fmt.Sprintf("request_bytes <= $%d", len(args)))
	}
	if query.ResponseBytesMin != nil {
		args = append(args, *query.ResponseBytesMin)
		clauses = append(clauses, fmt.Sprintf("response_bytes >= $%d", len(args)))
	}
	if query.ResponseBytesMax != nil {
		args = append(args, *query.ResponseBytesMax)
		clauses = append(clauses, fmt.Sprintf("response_bytes <= $%d", len(args)))
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func flattenAPIErrors(items []*interfaces.ErrorMessage) []upstreamError {
	out := make([]upstreamError, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		msg := ""
		if item.Error != nil {
			msg = item.Error.Error()
		}
		out = append(out, upstreamError{StatusCode: item.StatusCode, Message: msg})
	}
	return out
}

func cloneHeaders(headers map[string][]string) map[string][]string {
	if len(headers) == 0 {
		return map[string][]string{}
	}
	out := make(map[string][]string, len(headers))
	for key, values := range headers {
		copied := make([]string, len(values))
		copy(copied, values)
		out[key] = copied
	}
	return out
}

func stripQuery(raw string) string {
	if idx := strings.IndexByte(raw, '?'); idx >= 0 {
		return raw[:idx]
	}
	return raw
}

func hasPayload(payload []byte) bool {
	return len(bytes.TrimSpace(payload)) > 0
}

func inferDownstreamTransport(headers map[string][]string, websocketTimeline []byte) string {
	if hasPayload(websocketTimeline) {
		return "websocket"
	}
	for key, values := range headers {
		if strings.EqualFold(strings.TrimSpace(key), "Upgrade") {
			for _, value := range values {
				if strings.EqualFold(strings.TrimSpace(value), "websocket") {
					return "websocket"
				}
			}
		}
	}
	return "http"
}

func inferUpstreamTransport(apiRequest, apiResponse, apiWebsocketTimeline []byte) string {
	hasHTTP := hasPayload(apiRequest) || hasPayload(apiResponse)
	hasWS := hasPayload(apiWebsocketTimeline)
	switch {
	case hasHTTP && hasWS:
		return "websocket+http"
	case hasWS:
		return "websocket"
	case hasHTTP:
		return "http"
	default:
		return ""
	}
}

func extractAuthMetadata(apiRequest []byte) (string, string, string) {
	text := string(apiRequest)
	provider := extractPattern(authProviderPattern, text)
	authID := extractPattern(authIDPattern, text)
	authType := extractPattern(authTypePattern, text)
	return provider, authID, authType
}

func extractPattern(re *regexp.Regexp, text string) string {
	match := re.FindStringSubmatch(text)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(match[1])
}

func extractModel(body, apiRequest []byte) string {
	for _, path := range []string{"model", "model_id"} {
		if result := gjson.GetBytes(body, path); result.Exists() {
			value := strings.TrimSpace(result.String())
			if value != "" {
				return value
			}
		}
	}
	upstreamURL := extractPattern(upstreamURLPattern, string(apiRequest))
	return extractModelFromURL(upstreamURL)
}

func extractModelFromURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	path := parsed.Path
	marker := "/models/"
	idx := strings.Index(path, marker)
	if idx == -1 {
		return ""
	}
	rest := path[idx+len(marker):]
	if rest == "" {
		return ""
	}
	for i, ch := range rest {
		if ch == ':' || ch == '/' {
			return rest[:i]
		}
	}
	return rest
}

func gzipBytes(payload []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(payload); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func gunzipBytes(payload []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func decompressResponseLimited(responseHeaders map[string][]string, response []byte, maxBytes int) ([]byte, bool, error) {
	if len(responseHeaders) == 0 || len(response) == 0 {
		payload, truncated := truncatePayload(response, maxBytes)
		return payload, truncated, nil
	}
	contentEncoding := ""
	for key, values := range responseHeaders {
		if strings.EqualFold(key, "Content-Encoding") && len(values) > 0 {
			contentEncoding = strings.ToLower(strings.TrimSpace(values[0]))
			break
		}
	}
	switch contentEncoding {
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(response))
		if err != nil {
			return nil, false, err
		}
		defer reader.Close()
		return readLimited(reader, maxBytes)
	case "deflate":
		reader := flate.NewReader(bytes.NewReader(response))
		defer reader.Close()
		return readLimited(reader, maxBytes)
	case "br":
		return readLimited(brotli.NewReader(bytes.NewReader(response)), maxBytes)
	case "zstd":
		reader, err := zstd.NewReader(bytes.NewReader(response))
		if err != nil {
			return nil, false, err
		}
		defer reader.Close()
		return readLimited(reader, maxBytes)
	default:
		payload, truncated := truncatePayload(response, maxBytes)
		return payload, truncated, nil
	}
}

func responseHasContentEncoding(responseHeaders map[string][]string) bool {
	for key, values := range responseHeaders {
		if strings.EqualFold(key, "Content-Encoding") && len(values) > 0 && strings.TrimSpace(values[0]) != "" {
			return true
		}
	}
	return false
}

func readLimited(reader io.Reader, maxBytes int) ([]byte, bool, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, int64(maxBytes)+1))
	if err != nil {
		return nil, false, err
	}
	if len(payload) > maxBytes {
		return payload[:maxBytes], true, nil
	}
	return payload, false, nil
}
