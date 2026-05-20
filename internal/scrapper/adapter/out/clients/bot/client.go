package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/resilience"
)

// Client is an HTTP client for bot service.
type Client struct {
	http      *http.Client
	resilient *resilience.Client
	baseURL   string
}

func NewClient(baseURL string, rc *resilience.Client) (*Client, error) {
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("invalid base url: %w", err)
	}
	return &Client{
		http:      &http.Client{},
		resilient: rc,
		baseURL:   baseURL,
	}, nil
}

// SendUpdate sends a link update notification to the bot.
func (c *Client) SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error {
	body, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal update: %w", err)
	}

	resp, err := c.resilient.Execute(ctx, func(ctx context.Context) (*http.Response, error) {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/updates", bytes.NewReader(body))
		if reqErr != nil {
			return nil, fmt.Errorf("create request: %w", reqErr)
		}
		req.Header.Set("Content-Type", "application/json")
		return c.http.Do(req) //nolint:gosec // baseURL is validated in constructor
	})
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			_ = err
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bot api status: %d", resp.StatusCode)
	}

	return nil
}
