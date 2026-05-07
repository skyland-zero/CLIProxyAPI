package pricing

import (
	"bytes"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var pricePattern = regexp.MustCompile(`\$([0-9]+(?:\.[0-9]+)?)`)

func ParseOpenAIPricesHTML(data []byte, sourceURL string) ([]ModelPrice, error) {
	return ParseOpenAIPricesTokens(htmlTextTokens(data), sourceURL), nil
}

func ParseOpenAIPricesText(text string, sourceURL string) []ModelPrice {
	lines := strings.Split(text, "\n")
	tokens := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			tokens = append(tokens, line)
		}
	}
	return ParseOpenAIPricesTokens(tokens, sourceURL)
}

func ParseOpenAIPricesTokens(tokens []string, sourceURL string) []ModelPrice {
	now := time.Now().UTC()
	prices := make([]ModelPrice, 0, 32)
	currentModel := ""
	for i := 0; i < len(tokens); i++ {
		token := cleanToken(tokens[i])
		if isOpenAIModelName(token) {
			currentModel = token
			if i+1 >= len(tokens) {
				continue
			}
			next := cleanToken(tokens[i+1])
			if isModality(next) {
				parsed, consumed := parseModalityPrice(tokens, i+1, currentModel, sourceURL, now)
				prices = append(prices, parsed...)
				i += consumed
				continue
			}
			if isPriceValue(next) || isDash(next) {
				parsed, consumed := parseModelPrice(tokens, i+1, currentModel, sourceURL, now)
				prices = append(prices, parsed...)
				i += consumed
			}
			continue
		}
		if currentModel != "" && isModality(token) {
			parsed, consumed := parseModalityPrice(tokens, i, currentModel, sourceURL, now)
			prices = append(prices, parsed...)
			i += consumed
		}
	}
	return dedupePrices(prices)
}

func parseModelPrice(tokens []string, start int, model, sourceURL string, now time.Time) ([]ModelPrice, int) {
	values := collectPriceValues(tokens, start, 6)
	if len(values) == 0 {
		return nil, 0
	}
	if strings.Contains(strings.ToLower(cleanToken(tokens[start])), "/ hour") && len(values) >= 4 {
		return []ModelPrice{basePrice(model, "fine_tuning", "standard", "text", sourceURL, now, values[1], values[2], values[3], values[0], 0)}, len(values)
	}
	prices := make([]ModelPrice, 0, 2)
	if len(values) >= 3 {
		prices = append(prices, basePrice(model, "standard", "short_context", "text", sourceURL, now, values[0], values[1], values[2], 0, 0))
	}
	if len(values) >= 6 && (values[3] != 0 || values[4] != 0 || values[5] != 0) {
		prices = append(prices, basePrice(model, "standard", "long_context", "text", sourceURL, now, values[3], values[4], values[5], 0, 0))
	}
	return prices, len(values)
}

func parseModalityPrice(tokens []string, start int, model, sourceURL string, now time.Time) ([]ModelPrice, int) {
	modality := cleanToken(tokens[start])
	values := collectPriceValues(tokens, start+1, 3)
	if len(values) < 3 {
		return nil, 0
	}
	return []ModelPrice{basePrice(model, "multimodal", "standard", normalizeModality(modality), sourceURL, now, values[0], values[1], values[2], 0, 0)}, len(values)
}

func collectPriceValues(tokens []string, start, max int) []float64 {
	values := make([]float64, 0, max)
	for i := start; i < len(tokens) && len(values) < max; i++ {
		token := cleanToken(tokens[i])
		if isDash(token) {
			values = append(values, 0)
			continue
		}
		if !isPriceValue(token) {
			break
		}
		values = append(values, parsePrice(token))
	}
	return values
}

func basePrice(model, category, contextName, modality, sourceURL string, now time.Time, input, cached, output, training, perSecond float64) ModelPrice {
	return ModelPrice{
		Provider:         ProviderOpenAI,
		Model:            model,
		Category:         category,
		Context:          contextName,
		Modality:         modality,
		Unit:             "1m_tokens",
		InputPer1M:       input,
		CachedInputPer1M: cached,
		OutputPer1M:      output,
		TrainingPerHour:  training,
		PricePerSecond:   perSecond,
		SourceURL:        sourceURL,
		FetchedAt:        now,
		UpdatedAt:        now,
	}
}

func dedupePrices(prices []ModelPrice) []ModelPrice {
	seen := make(map[string]ModelPrice, len(prices))
	for _, price := range prices {
		price = normalizePrice(price, price.UpdatedAt)
		seen[priceKey(price.Provider, price.Model, price.Category, price.Context, price.Modality, price.Unit)] = price
	}
	out := make([]ModelPrice, 0, len(seen))
	for _, price := range seen {
		out = append(out, price)
	}
	return out
}

func cleanToken(token string) string {
	token = strings.TrimSpace(token)
	token = strings.ReplaceAll(token, "\\-", "-")
	token = strings.ReplaceAll(token, "`", "")
	token = strings.Join(strings.Fields(token), " ")
	return token
}

func isOpenAIModelName(token string) bool {
	token = strings.ToLower(cleanToken(token))
	if strings.Contains(token, " ") {
		return false
	}
	return strings.HasPrefix(token, "gpt-") || strings.HasPrefix(token, "o") || token == "chat-latest"
}

func isModality(token string) bool {
	switch strings.ToLower(cleanToken(token)) {
	case "audio", "text", "image omitted", "image":
		return true
	default:
		return false
	}
}

func normalizeModality(modality string) string {
	modality = strings.ToLower(cleanToken(modality))
	if strings.HasPrefix(modality, "image") {
		return "image"
	}
	return modality
}

func isDash(token string) bool {
	token = cleanToken(token)
	return token == "-" || token == "–" || token == "—"
}

func isPriceValue(token string) bool {
	return pricePattern.MatchString(cleanToken(token))
}

func parsePrice(token string) float64 {
	match := pricePattern.FindStringSubmatch(cleanToken(token))
	if len(match) != 2 {
		return 0
	}
	value, _ := strconv.ParseFloat(match[1], 64)
	return value
}

func ParseOpenAIPricesMarkdown(data []byte, sourceURL string) []ModelPrice {
	return ParseOpenAIPricesText(string(bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))), sourceURL)
}
