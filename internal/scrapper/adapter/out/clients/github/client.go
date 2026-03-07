// Package github provides GitHub API client.
package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Client is a GitHub API client.
type Client struct {
	http    *http.Client
	baseURL string
	token   string
}

const defaultTimeout = 10 * time.Second

// NewClient creates a new GitHub client.
func NewClient(token string) *Client {
	return &Client{
		http:    &http.Client{Timeout: defaultTimeout},
		baseURL: "https://api.github.com",
		token:   token,
	}
}

type repoResponse struct {
	UpdatedAt time.Time `json:"updated_at"`
}

// GetLastUpdated returns last updated time for a GitHub repo URL.
func (c *Client) GetLastUpdated(ctx context.Context, url string) (time.Time, error) {
	path, err := parseGitHubPath(url)
	if err != nil {
		return time.Time{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github+json")

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req) //nolint:gosec // baseURL is hardcoded
	if err != nil {
		return time.Time{}, fmt.Errorf("do request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			_ = err
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("github api status: %d", resp.StatusCode)
	}

	var result repoResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return time.Time{}, fmt.Errorf("decode response: %w", err)
	}

	return result.UpdatedAt, nil
}

// https://github.com/golang/go → /repos/golang/go
func parseGitHubPath(url string) (string, error) {
	url = strings.TrimPrefix(url, "https://github.com")

	parts := strings.Split(strings.Trim(url, "/"), "/")
	const minPathParts = 2
	if len(parts) < minPathParts {
		return "", fmt.Errorf("invalid github url: %s", url)
	}

	return "/repos/" + parts[0] + "/" + parts[1], nil
}

// Supports returns true if this client can handle the given URL.
func (c *Client) Supports(url string) bool {
	return strings.HasPrefix(url, "https://github.com")
}
