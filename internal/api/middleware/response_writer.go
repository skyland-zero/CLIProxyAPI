// Package middleware provides Gin HTTP middleware for the CLI Proxy API server.
// It includes a sophisticated response writer wrapper designed to capture and log request and response data,
// including support for streaming responses, without impacting latency.
package middleware

import (
	"bytes"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
)

const requestBodyOverrideContextKey = "REQUEST_BODY_OVERRIDE"
const responseBodyOverrideContextKey = "RESPONSE_BODY_OVERRIDE"
const websocketTimelineOverrideContextKey = "WEBSOCKET_TIMELINE_OVERRIDE"
const maxCapturedResponseBodyBytes = 4 << 20   // 4 MiB
const maxStreamingQueuedChunkBytes = 256 << 10 // 256 KiB

// RequestInfo holds essential details of an incoming HTTP request for logging purposes.
type RequestInfo struct {
	URL           string              // URL is the request URL.
	Method        string              // Method is the HTTP method (e.g., GET, POST).
	Headers       map[string][]string // Headers contains the request headers.
	Body          []byte              // Body is the raw request body.
	BodyBytes     int64               // BodyBytes is the original request body size when known.
	BodyTruncated bool                // BodyTruncated indicates the request body was not fully captured.
	RequestID     string              // RequestID is the unique identifier for the request.
	Timestamp     time.Time           // Timestamp is when the request was received.
}

// ResponseWriterWrapper wraps the standard gin.ResponseWriter to intercept and log response data.
// It is designed to handle both standard and streaming responses, ensuring that logging operations do not block the client response.
type ResponseWriterWrapper struct {
	gin.ResponseWriter
	body                *bytes.Buffer              // body is a buffer to store the response body for non-streaming responses.
	isStreaming         bool                       // isStreaming indicates whether the response is a streaming type (e.g., text/event-stream).
	streamWriter        logging.StreamingLogWriter // streamWriter is a writer for handling streaming log entries.
	chunkChannel        chan []byte                // chunkChannel is a channel for asynchronously passing response chunks to the logger.
	streamDone          chan struct{}              // streamDone signals when the streaming goroutine completes.
	logger              logging.RequestLogger      // logger is the instance of the request logger service.
	requestInfo         *RequestInfo               // requestInfo holds the details of the original request.
	statusCode          int                        // statusCode stores the HTTP status code of the response.
	headers             map[string][]string        // headers stores the response headers.
	logOnErrorOnly      bool                       // logOnErrorOnly enables logging only when an error response is detected.
	firstChunkTimestamp time.Time                  // firstChunkTimestamp captures TTFB for streaming responses.
	bodyTruncated       bool                       // bodyTruncated indicates the captured response body reached the memory cap.
	streamTruncated     bool                       // streamTruncated indicates streaming log chunks were truncated or dropped.
	responseBytesSeen   int64                      // responseBytesSeen tracks bytes written to the client.
}

// NewResponseWriterWrapper creates and initializes a new ResponseWriterWrapper.
// It takes the original gin.ResponseWriter, a logger instance, and request information.
//
// Parameters:
//   - w: The original gin.ResponseWriter to wrap.
//   - logger: The logging service to use for recording requests.
//   - requestInfo: The pre-captured information about the incoming request.
//
// Returns:
//   - A pointer to a new ResponseWriterWrapper.
func NewResponseWriterWrapper(w gin.ResponseWriter, logger logging.RequestLogger, requestInfo *RequestInfo) *ResponseWriterWrapper {
	return &ResponseWriterWrapper{
		ResponseWriter: w,
		body:           &bytes.Buffer{},
		logger:         logger,
		requestInfo:    requestInfo,
		headers:        make(map[string][]string),
	}
}

// Write wraps the underlying ResponseWriter's Write method to capture response data.
// For non-streaming responses, it writes to an internal buffer. For streaming responses,
// it sends data chunks to a non-blocking channel for asynchronous logging.
// CRITICAL: This method prioritizes writing to the client to ensure zero latency,
// handling logging operations subsequently.
func (w *ResponseWriterWrapper) Write(data []byte) (int, error) {
	// Ensure headers are captured before first write
	// This is critical because Write() may trigger WriteHeader() internally
	w.ensureHeadersCaptured()
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	w.ensureStreamingInitialized(w.statusCode)

	// CRITICAL: Write to client first (zero latency)
	n, err := w.ResponseWriter.Write(data)
	w.responseBytesSeen += int64(n)

	// THEN: Handle logging based on response type
	if w.isStreaming && w.chunkChannel != nil {
		// Capture TTFB on first chunk (synchronous, before async channel send)
		if w.firstChunkTimestamp.IsZero() {
			w.firstChunkTimestamp = time.Now()
		}
		if n > 0 {
			w.enqueueStreamingChunk(data[:n])
		}
		return n, err
	}

	if n > 0 && w.shouldBufferResponseBody() {
		w.appendResponseBody(data[:n])
	}

	return n, err
}

func (w *ResponseWriterWrapper) shouldBufferResponseBody() bool {
	if w.body != nil && w.body.Len() >= maxCapturedResponseBodyBytes {
		w.bodyTruncated = true
		return false
	}
	if w.logger != nil && w.logger.IsEnabled() {
		return true
	}
	if !w.logOnErrorOnly {
		return false
	}
	status := w.statusCode
	if status == 0 {
		if statusWriter, ok := w.ResponseWriter.(interface{ Status() int }); ok && statusWriter != nil {
			status = statusWriter.Status()
		} else {
			status = http.StatusOK
		}
	}
	return status >= http.StatusBadRequest
}

// WriteString wraps the underlying ResponseWriter's WriteString method to capture response data.
// Some handlers (and fmt/io helpers) write via io.StringWriter; without this override, those writes
// bypass Write() and would be missing from request logs.
func (w *ResponseWriterWrapper) WriteString(data string) (int, error) {
	w.ensureHeadersCaptured()
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	w.ensureStreamingInitialized(w.statusCode)

	// CRITICAL: Write to client first (zero latency)
	n, err := w.ResponseWriter.WriteString(data)
	w.responseBytesSeen += int64(n)

	// THEN: Capture for logging
	if w.isStreaming && w.chunkChannel != nil {
		// Capture TTFB on first chunk (synchronous, before async channel send)
		if w.firstChunkTimestamp.IsZero() {
			w.firstChunkTimestamp = time.Now()
		}
		if n > 0 {
			w.enqueueStreamingChunk([]byte(data[:n]))
		}
		return n, err
	}

	if n > 0 && w.shouldBufferResponseBody() {
		w.appendResponseString(data[:n])
	}
	return n, err
}

func (w *ResponseWriterWrapper) enqueueStreamingChunk(data []byte) {
	if w.chunkChannel == nil || len(data) == 0 {
		return
	}
	payload := data
	if len(payload) > maxStreamingQueuedChunkBytes {
		payload = payload[:maxStreamingQueuedChunkBytes]
		w.streamTruncated = true
	}
	select {
	case w.chunkChannel <- bytes.Clone(payload):
	default:
		w.streamTruncated = true
	}
}

func (w *ResponseWriterWrapper) appendResponseBody(data []byte) {
	if w.body == nil || len(data) == 0 {
		return
	}
	remaining := maxCapturedResponseBodyBytes - w.body.Len()
	if remaining <= 0 {
		w.bodyTruncated = true
		return
	}
	if len(data) > remaining {
		_, _ = w.body.Write(data[:remaining])
		w.bodyTruncated = true
		return
	}
	_, _ = w.body.Write(data)
}

func (w *ResponseWriterWrapper) appendResponseString(data string) {
	if w.body == nil || data == "" {
		return
	}
	remaining := maxCapturedResponseBodyBytes - w.body.Len()
	if remaining <= 0 {
		w.bodyTruncated = true
		return
	}
	if len(data) > remaining {
		_, _ = w.body.WriteString(data[:remaining])
		w.bodyTruncated = true
		return
	}
	_, _ = w.body.WriteString(data)
}

// WriteHeader wraps the underlying ResponseWriter's WriteHeader method.
// It captures the status code, detects if the response is streaming based on the Content-Type header,
// and initializes the appropriate logging mechanism (standard or streaming).
func (w *ResponseWriterWrapper) WriteHeader(statusCode int) {
	w.statusCode = statusCode

	// Capture response headers using the new method
	w.captureCurrentHeaders()
	w.ensureStreamingInitialized(statusCode)

	// Call original WriteHeader
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *ResponseWriterWrapper) ensureStreamingInitialized(statusCode int) {
	if w.streamWriter != nil || w.chunkChannel != nil {
		return
	}

	// Detect streaming based on Content-Type
	contentType := w.ResponseWriter.Header().Get("Content-Type")
	w.isStreaming = w.detectStreaming(contentType)

	// If streaming, initialize streaming log writer
	if w.isStreaming && w.logger != nil && w.logger.IsEnabled() {
		streamWriter, err := w.openStreamingLogWriter()
		if err == nil {
			w.streamWriter = streamWriter
			w.chunkChannel = make(chan []byte, 32) // Buffered channel for async writes
			doneChan := make(chan struct{})
			w.streamDone = doneChan

			// Start async chunk processor
			go w.processStreamingChunks(doneChan)

			// Write status immediately
			_ = streamWriter.WriteStatus(statusCode, w.headers)
		}
	}
}

func (w *ResponseWriterWrapper) openStreamingLogWriter() (logging.StreamingLogWriter, error) {
	requestBytes := int64(0)
	requestBodyTruncated := false
	if w.requestInfo != nil {
		requestBytes = w.requestInfo.BodyBytes
		requestBodyTruncated = w.requestInfo.BodyTruncated
		if requestBytes <= 0 && len(w.requestInfo.Body) > 0 {
			requestBytes = int64(len(w.requestInfo.Body))
		}
		if loggerWithCaptureInfo, ok := w.logger.(interface {
			LogStreamingRequestWithCaptureInfo(string, string, map[string][]string, []byte, bool, int64, string) (logging.StreamingLogWriter, error)
		}); ok {
			return loggerWithCaptureInfo.LogStreamingRequestWithCaptureInfo(w.requestInfo.URL, w.requestInfo.Method, w.requestInfo.Headers, w.requestInfo.Body, requestBodyTruncated, requestBytes, w.requestInfo.RequestID)
		}
		return w.logger.LogStreamingRequest(w.requestInfo.URL, w.requestInfo.Method, w.requestInfo.Headers, w.requestInfo.Body, w.requestInfo.RequestID)
	}
	return w.logger.LogStreamingRequest("", "", nil, nil, "")
}

// ensureHeadersCaptured is a helper function to make sure response headers are captured.
// It is safe to call this method multiple times; it will always refresh the headers
// with the latest state from the underlying ResponseWriter.
func (w *ResponseWriterWrapper) ensureHeadersCaptured() {
	// Always capture the current headers to ensure we have the latest state
	w.captureCurrentHeaders()
}

// captureCurrentHeaders reads all headers from the underlying ResponseWriter and stores them
// in the wrapper's headers map. It creates copies of the header values to prevent race conditions.
func (w *ResponseWriterWrapper) captureCurrentHeaders() {
	// Initialize headers map if needed
	if w.headers == nil {
		w.headers = make(map[string][]string)
	}

	// Capture all current headers from the underlying ResponseWriter
	for key, values := range w.ResponseWriter.Header() {
		// Make a copy of the values slice to avoid reference issues
		headerValues := make([]string, len(values))
		copy(headerValues, values)
		w.headers[key] = headerValues
	}
}

// detectStreaming determines if a response should be treated as a streaming response.
// It checks for a "text/event-stream" Content-Type or a '"stream": true'
// field in the original request body.
func (w *ResponseWriterWrapper) detectStreaming(contentType string) bool {
	// Check Content-Type for Server-Sent Events
	if strings.Contains(contentType, "text/event-stream") {
		return true
	}

	// If a concrete Content-Type is already set (e.g., application/json for error responses),
	// treat it as non-streaming instead of inferring from the request payload.
	if strings.TrimSpace(contentType) != "" {
		return false
	}

	// Only fall back to request payload hints when Content-Type is not set yet.
	if w.requestInfo != nil && len(w.requestInfo.Body) > 0 {
		return bytes.Contains(w.requestInfo.Body, []byte(`"stream": true`)) ||
			bytes.Contains(w.requestInfo.Body, []byte(`"stream":true`))
	}

	return false
}

// processStreamingChunks runs in a separate goroutine to process response chunks from the chunkChannel.
// It asynchronously writes each chunk to the streaming log writer.
func (w *ResponseWriterWrapper) processStreamingChunks(done chan struct{}) {
	if done == nil {
		return
	}

	defer close(done)

	if w.streamWriter == nil || w.chunkChannel == nil {
		return
	}

	for chunk := range w.chunkChannel {
		w.streamWriter.WriteChunkAsync(chunk)
	}
}

// Finalize completes the logging process for the request and response.
// For streaming responses, it closes the chunk channel and the stream writer.
// For non-streaming responses, it logs the complete request and response details,
// including any API-specific request/response data stored in the Gin context.
func (w *ResponseWriterWrapper) Finalize(c *gin.Context) error {
	if w.logger == nil {
		return nil
	}

	finalStatusCode := w.statusCode
	if finalStatusCode == 0 {
		if statusWriter, ok := w.ResponseWriter.(interface{ Status() int }); ok {
			finalStatusCode = statusWriter.Status()
		} else {
			finalStatusCode = 200
		}
	}

	var slicesAPIResponseError []*interfaces.ErrorMessage
	apiResponseError, isExist := c.Get("API_RESPONSE_ERROR")
	if isExist {
		if apiErrors, ok := apiResponseError.([]*interfaces.ErrorMessage); ok {
			slicesAPIResponseError = apiErrors
		}
	}

	hasAPIError := len(slicesAPIResponseError) > 0 || finalStatusCode >= http.StatusBadRequest
	forceLog := w.logOnErrorOnly && hasAPIError && !w.logger.IsEnabled()
	if !w.logger.IsEnabled() && !forceLog {
		return nil
	}

	if w.isStreaming && w.streamWriter != nil {
		if w.chunkChannel != nil {
			close(w.chunkChannel)
			w.chunkChannel = nil
		}

		if w.streamDone != nil {
			<-w.streamDone
			w.streamDone = nil
		}

		w.streamWriter.SetFirstChunkTimestamp(w.firstChunkTimestamp)

		// Write API Request and Response to the streaming log before closing
		apiRequest := w.extractAPIRequest(c)
		if len(apiRequest) > 0 {
			_ = w.streamWriter.WriteAPIRequest(apiRequest)
		}
		apiResponse := w.extractAPIResponse(c)
		if len(apiResponse) > 0 {
			_ = w.streamWriter.WriteAPIResponse(apiResponse)
		}
		apiWebsocketTimeline := w.extractAPIWebsocketTimeline(c)
		if len(apiWebsocketTimeline) > 0 {
			_ = w.streamWriter.WriteAPIWebsocketTimeline(apiWebsocketTimeline)
		}
		if w.streamTruncated {
			if marker, ok := w.streamWriter.(interface{ MarkResponseTruncated() }); ok {
				marker.MarkResponseTruncated()
			}
		}
		if err := w.streamWriter.Close(); err != nil {
			w.streamWriter = nil
			return err
		}
		w.streamWriter = nil
		return nil
	}

	return w.logRequest(w.extractRequestBody(c), finalStatusCode, w.cloneHeaders(), w.extractResponseBody(c), w.extractWebsocketTimeline(c), w.extractAPIRequest(c), w.extractAPIResponse(c), w.extractAPIWebsocketTimeline(c), w.extractAPIResponseTimestamp(c), slicesAPIResponseError, forceLog)
}

func (w *ResponseWriterWrapper) cloneHeaders() map[string][]string {
	w.ensureHeadersCaptured()

	finalHeaders := make(map[string][]string, len(w.headers))
	for key, values := range w.headers {
		headerValues := make([]string, len(values))
		copy(headerValues, values)
		finalHeaders[key] = headerValues
	}

	return finalHeaders
}

func (w *ResponseWriterWrapper) extractAPIRequest(c *gin.Context) []byte {
	apiRequest, isExist := c.Get("API_REQUEST")
	if !isExist {
		return nil
	}
	data, ok := apiRequest.([]byte)
	if !ok || len(data) == 0 {
		return nil
	}
	return data
}

func (w *ResponseWriterWrapper) extractAPIResponse(c *gin.Context) []byte {
	apiResponse, isExist := c.Get("API_RESPONSE")
	if !isExist {
		return nil
	}
	data, ok := apiResponse.([]byte)
	if !ok || len(data) == 0 {
		return nil
	}
	return data
}

func (w *ResponseWriterWrapper) extractAPIWebsocketTimeline(c *gin.Context) []byte {
	apiTimeline, isExist := c.Get("API_WEBSOCKET_TIMELINE")
	if !isExist {
		return nil
	}
	data, ok := apiTimeline.([]byte)
	if !ok || len(data) == 0 {
		return nil
	}
	return bytes.Clone(data)
}

func (w *ResponseWriterWrapper) extractAPIResponseTimestamp(c *gin.Context) time.Time {
	ts, isExist := c.Get("API_RESPONSE_TIMESTAMP")
	if !isExist {
		return time.Time{}
	}
	if t, ok := ts.(time.Time); ok {
		return t
	}
	return time.Time{}
}

func (w *ResponseWriterWrapper) extractRequestBody(c *gin.Context) []byte {
	if body := extractBodyOverride(c, requestBodyOverrideContextKey); len(body) > 0 {
		return body
	}
	if w.requestInfo != nil && len(w.requestInfo.Body) > 0 {
		return w.requestInfo.Body
	}
	return nil
}

func (w *ResponseWriterWrapper) extractResponseBody(c *gin.Context) []byte {
	if body := extractBodyOverride(c, responseBodyOverrideContextKey); len(body) > 0 {
		return body
	}
	if w.body == nil || w.body.Len() == 0 {
		return nil
	}
	return bytes.Clone(w.body.Bytes())
}

func (w *ResponseWriterWrapper) extractWebsocketTimeline(c *gin.Context) []byte {
	return extractBodyOverride(c, websocketTimelineOverrideContextKey)
}

func extractBodyOverride(c *gin.Context, key string) []byte {
	if c == nil {
		return nil
	}
	bodyOverride, isExist := c.Get(key)
	if !isExist {
		return nil
	}
	switch value := bodyOverride.(type) {
	case []byte:
		if len(value) > 0 {
			return bytes.Clone(value)
		}
	case string:
		if strings.TrimSpace(value) != "" {
			return []byte(value)
		}
	}
	return nil
}

func (w *ResponseWriterWrapper) logRequest(requestBody []byte, statusCode int, headers map[string][]string, body, websocketTimeline, apiRequestBody, apiResponseBody, apiWebsocketTimeline []byte, apiResponseTimestamp time.Time, apiResponseErrors []*interfaces.ErrorMessage, forceLog bool) error {
	if w.requestInfo == nil {
		return nil
	}
	requestBytes := w.requestInfo.BodyBytes
	if requestBytes <= 0 && len(requestBody) > 0 {
		requestBytes = int64(len(requestBody))
	}
	responseBytes := w.responseBytesSeen
	if responseBytes <= 0 && len(body) > 0 {
		responseBytes = int64(len(body))
	}

	if loggerWithCaptureInfo, ok := w.logger.(interface {
		LogRequestWithCaptureInfo(string, string, map[string][]string, []byte, bool, int64, int, map[string][]string, []byte, bool, int64, []byte, []byte, []byte, []byte, []*interfaces.ErrorMessage, bool, string, time.Time, time.Time) error
	}); ok {
		return loggerWithCaptureInfo.LogRequestWithCaptureInfo(
			w.requestInfo.URL,
			w.requestInfo.Method,
			w.requestInfo.Headers,
			requestBody,
			w.requestInfo.BodyTruncated,
			requestBytes,
			statusCode,
			headers,
			body,
			w.bodyTruncated,
			responseBytes,
			websocketTimeline,
			apiRequestBody,
			apiResponseBody,
			apiWebsocketTimeline,
			apiResponseErrors,
			forceLog,
			w.requestInfo.RequestID,
			w.requestInfo.Timestamp,
			apiResponseTimestamp,
		)
	}

	if loggerWithOptions, ok := w.logger.(interface {
		LogRequestWithOptions(string, string, map[string][]string, []byte, int, map[string][]string, []byte, []byte, []byte, []byte, []byte, []*interfaces.ErrorMessage, bool, string, time.Time, time.Time) error
	}); ok {
		return loggerWithOptions.LogRequestWithOptions(
			w.requestInfo.URL,
			w.requestInfo.Method,
			w.requestInfo.Headers,
			requestBody,
			statusCode,
			headers,
			body,
			websocketTimeline,
			apiRequestBody,
			apiResponseBody,
			apiWebsocketTimeline,
			apiResponseErrors,
			forceLog,
			w.requestInfo.RequestID,
			w.requestInfo.Timestamp,
			apiResponseTimestamp,
		)
	}

	return w.logger.LogRequest(
		w.requestInfo.URL,
		w.requestInfo.Method,
		w.requestInfo.Headers,
		requestBody,
		statusCode,
		headers,
		body,
		websocketTimeline,
		apiRequestBody,
		apiResponseBody,
		apiWebsocketTimeline,
		apiResponseErrors,
		w.requestInfo.RequestID,
		w.requestInfo.Timestamp,
		apiResponseTimestamp,
	)
}
