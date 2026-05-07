package pricing

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"golang.org/x/net/html"
)

func FetchOpenAIPrices(ctx context.Context, url string) ([]ModelPrice, error) {
	if url == "" {
		url = OpenAIPricingURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("pricing: create OpenAI pricing request: %w", err)
	}
	req.Header.Set("User-Agent", "CLIProxyAPI pricing fetcher")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pricing: fetch OpenAI pricing: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("pricing: OpenAI pricing returned status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("pricing: read OpenAI pricing response: %w", err)
	}
	return ParseOpenAIPricesHTML(data, url)
}

func htmlTextTokens(data []byte) []string {
	tokenizer := html.NewTokenizer(bytes.NewReader(data))
	tokens := make([]string, 0, 256)
	for {
		typeToken := tokenizer.Next()
		switch typeToken {
		case html.ErrorToken:
			return tokens
		case html.TextToken:
			text := strings.TrimSpace(html.UnescapeString(string(tokenizer.Text())))
			if text != "" {
				tokens = append(tokens, text)
			}
		}
	}
}
