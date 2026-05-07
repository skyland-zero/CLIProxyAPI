package pricing

import (
	"context"
	"strings"

	usage "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

func EstimateEventCost(ctx context.Context, event usage.Event) (*float64, error) {
	store := DefaultStore()
	if store == nil {
		return nil, nil
	}
	price, ok, err := findEventPrice(ctx, store, event)
	if err != nil || !ok {
		return nil, err
	}
	cost := calculateCost(event, price)
	return &cost, nil
}

func PriceEvents(ctx context.Context, events []usage.Event) ([]PricedEvent, error) {
	out := make([]PricedEvent, 0, len(events))
	for _, event := range events {
		cost, err := EstimateEventCost(ctx, event)
		if err != nil {
			return nil, err
		}
		out = append(out, PricedEvent{Event: event, EstimatedCostUSD: cost})
	}
	return out, nil
}

func BuildPricedSummary(ctx context.Context, events []usage.Event, query usage.SummaryQuery) ([]usage.SummaryRow, error) {
	baseRows := usage.BuildSummaryForPricing(events, query.GroupBy, query.TimeZone)
	rows := make([]usage.SummaryRow, 0, len(baseRows))
	byGroup := make(map[string]*usage.SummaryRow, len(baseRows))
	for _, row := range baseRows {
		rows = append(rows, row)
		byGroup[row.Group] = &rows[len(rows)-1]
	}
	for _, event := range events {
		group := usage.GroupValueForPricing(event, query.GroupBy, query.TimeZone)
		row := byGroup[group]
		if row == nil {
			continue
		}
		cost, err := EstimateEventCost(ctx, event)
		if err != nil {
			return nil, err
		}
		if cost == nil {
			row.UnpricedRequests++
			continue
		}
		row.PricedRequests++
		if row.EstimatedCostUSD == nil {
			zero := 0.0
			row.EstimatedCostUSD = &zero
		}
		*row.EstimatedCostUSD += *cost
	}
	return rows, nil
}

func findEventPrice(ctx context.Context, store Store, event usage.Event) (ModelPrice, bool, error) {
	for _, model := range []string{strings.TrimSpace(event.Model), strings.TrimSpace(event.Alias)} {
		if model == "" {
			continue
		}
		price, ok, err := store.FindPrice(ctx, event.Provider, model)
		if err != nil || ok {
			return price, ok, err
		}
	}
	return ModelPrice{}, false, nil
}

func calculateCost(event usage.Event, price ModelPrice) float64 {
	tokens := event.Tokens
	return float64(tokens.InputTokens)/1_000_000*price.InputPer1M +
		float64(tokens.CachedTokens)/1_000_000*price.CachedInputPer1M +
		float64(tokens.OutputTokens+tokens.ReasoningTokens)/1_000_000*price.OutputPer1M
}
