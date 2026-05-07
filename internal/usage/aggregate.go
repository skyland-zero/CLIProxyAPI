package usage

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func buildSnapshot(events []Event) StatisticsSnapshot {
	snapshot := StatisticsSnapshot{
		APIs:           make(map[string]APISnapshot),
		RequestsByDay:  make(map[string]int64),
		RequestsByHour: make(map[string]int64),
		TokensByDay:    make(map[string]int64),
		TokensByHour:   make(map[string]int64),
	}
	apiStats := make(map[string]*apiAccumulator)
	for _, event := range events {
		tokens := normalizeTokens(event.Tokens)
		snapshot.TotalRequests++
		if event.Failed {
			snapshot.FailureCount++
		} else {
			snapshot.SuccessCount++
		}
		snapshot.TotalTokens += tokens.TotalTokens

		day := event.Timestamp.Format("2006-01-02")
		hour := fmt.Sprintf("%02d", event.Timestamp.Hour())
		snapshot.RequestsByDay[day]++
		snapshot.RequestsByHour[hour]++
		snapshot.TokensByDay[day] += tokens.TotalTokens
		snapshot.TokensByHour[hour] += tokens.TotalTokens

		apiKey := strings.TrimSpace(event.APIKeyHash)
		if apiKey == "" {
			apiKey = "unknown"
		}
		stats := apiStats[apiKey]
		if stats == nil {
			stats = &apiAccumulator{models: make(map[string]*modelAccumulator)}
			apiStats[apiKey] = stats
		}
		stats.totalRequests++
		stats.totalTokens += tokens.TotalTokens
		modelName := strings.TrimSpace(event.Model)
		if modelName == "" {
			modelName = "unknown"
		}
		model := stats.models[modelName]
		if model == nil {
			model = &modelAccumulator{}
			stats.models[modelName] = model
		}
		model.totalRequests++
		model.totalTokens += tokens.TotalTokens
		model.details = append(model.details, detailFromEvent(event))
	}

	for apiKey, stats := range apiStats {
		apiSnapshot := APISnapshot{
			TotalRequests: stats.totalRequests,
			TotalTokens:   stats.totalTokens,
			Models:        make(map[string]ModelSnapshot, len(stats.models)),
		}
		for modelName, model := range stats.models {
			apiSnapshot.Models[modelName] = ModelSnapshot{
				TotalRequests: model.totalRequests,
				TotalTokens:   model.totalTokens,
				Details:       append([]RequestDetail(nil), model.details...),
			}
		}
		snapshot.APIs[apiKey] = apiSnapshot
	}
	return snapshot
}

type apiAccumulator struct {
	totalRequests int64
	totalTokens   int64
	models        map[string]*modelAccumulator
}

type modelAccumulator struct {
	totalRequests int64
	totalTokens   int64
	details       []RequestDetail
}

func detailFromEvent(event Event) RequestDetail {
	return RequestDetail{
		Timestamp:  event.Timestamp,
		LatencyMs:  event.LatencyMs,
		Source:     event.Source,
		AuthIndex:  event.AuthIndex,
		AuthID:     event.AuthID,
		AuthType:   event.AuthType,
		RequestID:  event.RequestID,
		Endpoint:   event.Endpoint,
		APIKeyHash: event.APIKeyHash,
		Tokens:     normalizeTokens(event.Tokens),
		Failed:     event.Failed,
	}
}

func buildSummary(events []Event, groupBy string, timeZone string) []SummaryRow {
	groupBy = normalizeGroupBy(groupBy)
	location := summaryLocation(timeZone)
	rows := make(map[string]*SummaryRow)
	for _, event := range events {
		group := groupValue(event, groupBy, location)
		row := rows[group]
		if row == nil {
			row = &SummaryRow{Group: group}
			rows[group] = row
		}
		tokens := normalizeTokens(event.Tokens)
		row.TotalRequests++
		if event.Failed {
			row.FailureCount++
		} else {
			row.SuccessCount++
		}
		row.InputTokens += tokens.InputTokens
		row.OutputTokens += tokens.OutputTokens
		row.ReasoningTokens += tokens.ReasoningTokens
		row.CachedTokens += tokens.CachedTokens
		row.TotalTokens += tokens.TotalTokens
	}
	out := make([]SummaryRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalRequests == out[j].TotalRequests {
			return out[i].Group < out[j].Group
		}
		return out[i].TotalRequests > out[j].TotalRequests
	})
	return out
}

// BuildSummaryForPricing exposes usage grouping for cost estimation without duplicating grouping rules.
func BuildSummaryForPricing(events []Event, groupBy string, timeZone string) []SummaryRow {
	return buildSummary(events, groupBy, timeZone)
}

func normalizeGroupBy(groupBy string) string {
	switch strings.ToLower(strings.TrimSpace(groupBy)) {
	case "model", "alias", "auth_id", "auth_type", "source", "day", "hour", "api_key_hash":
		return strings.ToLower(strings.TrimSpace(groupBy))
	default:
		return "provider"
	}
}

func groupValue(event Event, groupBy string, location *time.Location) string {
	var value string
	switch groupBy {
	case "model":
		value = event.Model
	case "alias":
		value = event.Alias
	case "auth_id":
		value = event.AuthID
	case "auth_type":
		value = event.AuthType
	case "source":
		value = event.Source
	case "day":
		value = event.Timestamp.In(location).Format("2006-01-02")
	case "hour":
		value = event.Timestamp.In(location).Format("2006-01-02 15:00")
	case "api_key_hash":
		value = event.APIKeyHash
	default:
		value = event.Provider
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

// GroupValueForPricing exposes the normalized group key used by Summary.
func GroupValueForPricing(event Event, groupBy string, timeZone string) string {
	return groupValue(event, normalizeGroupBy(groupBy), summaryLocation(timeZone))
}

func eventMatches(event Event, query Query) bool {
	if !query.From.IsZero() && event.Timestamp.Before(query.From) {
		return false
	}
	if !query.To.IsZero() && event.Timestamp.After(query.To) {
		return false
	}
	if query.Provider != "" && !strings.EqualFold(event.Provider, query.Provider) {
		return false
	}
	if query.Model != "" && event.Model != query.Model {
		return false
	}
	if query.Alias != "" && event.Alias != query.Alias {
		return false
	}
	if query.AuthID != "" && event.AuthID != query.AuthID {
		return false
	}
	if query.AuthType != "" && !strings.EqualFold(event.AuthType, query.AuthType) {
		return false
	}
	if query.Source != "" && event.Source != query.Source {
		return false
	}
	if query.APIKeyHash != "" && event.APIKeyHash != query.APIKeyHash {
		return false
	}
	if query.Failed != nil && event.Failed != *query.Failed {
		return false
	}
	return true
}

func sortEventsDesc(events []Event) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].Timestamp.Equal(events[j].Timestamp) {
			return events[i].ID > events[j].ID
		}
		return events[i].Timestamp.After(events[j].Timestamp)
	})
}

func nowUTC() time.Time { return time.Now().UTC() }
