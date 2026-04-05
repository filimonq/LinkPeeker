// Package stackoverflow provides StackOverflow API client.
package stackoverflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Client is a StackOverflow API client.
type Client struct {
	http    *http.Client
	baseURL string
}

const defaultTimeout = 10 * time.Second

// NewClient creates a new StackOverflow client.
func NewClient() *Client {
	return &Client{
		http:    &http.Client{Timeout: defaultTimeout},
		baseURL: "https://api.stackexchange.com/2.3",
	}
}

// NewClientWithBaseURL creates a client with custom base URL for testing.
func NewClientWithBaseURL(baseURL string) *Client {
	return &Client{
		http:    &http.Client{Timeout: defaultTimeout},
		baseURL: baseURL,
	}
}

type soResponse struct {
	Items []struct {
		LastActivityDate int64 `json:"last_activity_date"`
	} `json:"items"`
}

// GetLastUpdated returns last activity time for a SO question URL.
func (c *Client) GetLastUpdated(ctx context.Context, url string) (time.Time, error) {
	questionID, err := parseStackOverflowQuestionID(url)
	if err != nil {
		return time.Time{}, err
	}

	apiURL := fmt.Sprintf("%s/questions/%s?site=stackoverflow", c.baseURL, questionID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.http.Do(req) //nolint:gosec // baseURL is hardcoded
	if err != nil {
		return time.Time{}, fmt.Errorf("do request: %w", err)
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			_ = err
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("stackoverflow api status: %d", resp.StatusCode)
	}

	var result soResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return time.Time{}, fmt.Errorf("decode response: %w", err)
	}

	if len(result.Items) == 0 {
		return time.Time{}, fmt.Errorf("question not found: %s", url)
	}

	return time.Unix(result.Items[0].LastActivityDate, 0), nil
}

// Supports returns true if this client can handle the given URL.
func (c *Client) Supports(url string) bool {
	return strings.HasPrefix(url, "https://stackoverflow.com")
}

func parseStackOverflowQuestionID(url string) (string, error) {
	url = strings.TrimPrefix(url, "https://stackoverflow.com/questions/")
	parts := strings.Split(url, "/")
	const minPathParts = 1
	if len(parts) < minPathParts || parts[0] == "" {
		return "", fmt.Errorf("invalid stackoverflow url: %s", url)
	}
	return parts[0], nil
}
