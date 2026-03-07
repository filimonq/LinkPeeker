// Package bot provides HTTP client for bot service.
package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

// Client is an HTTP client for bot service.
type Client struct {
	http    *http.Client
	baseURL string
}

const defaultTimeout = 10 * time.Second

// NewClient creates a new bot Client.
func NewClient(baseURL string) (*Client, error) {
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("invalid base url: %w", err)
	}
	return &Client{
		http:    &http.Client{Timeout: defaultTimeout},
		baseURL: baseURL,
	}, nil
}

// SendUpdate sends a link update notification to the bot.
func (c *Client) SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error {
	body, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal update: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/updates", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req) //nolint:gosec // baseURL is validated in constructor
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			_ = err
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bot api status: %d", resp.StatusCode)
	}

	return nil
}
