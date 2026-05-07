package management

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/pricing"
)

// GetOpenAIPricing returns the currently cached OpenAI model pricing rows.
func (h *Handler) GetOpenAIPricing(c *gin.Context) {
	prices, err := pricing.ListOpenAI(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to query pricing: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"mode":     pricing.Mode(),
		"provider": pricing.ProviderOpenAI,
		"prices":   prices,
	})
}

// RefreshOpenAIPricing refreshes OpenAI model pricing from the upstream pricing page.
func (h *Handler) RefreshOpenAIPricing(c *gin.Context) {
	prices, err := pricing.RefreshOpenAI(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("failed to refresh pricing: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"mode":     pricing.Mode(),
		"provider": pricing.ProviderOpenAI,
		"count":    len(prices),
		"prices":   prices,
	})
}
