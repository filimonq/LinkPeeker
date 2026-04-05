// Package github provides GitHub API client.
package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
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

// NewClientWithBaseURL creates a client with custom base URL for testing.
func NewClientWithBaseURL(token, baseURL string) *Client {
	return &Client{
		http:    &http.Client{Timeout: defaultTimeout},
		baseURL: baseURL,
		token:   token,
	}
}

type issueOrPRResponse struct {
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	User      struct {
		Login string `json:"login"`
	} `json:"user"`
}

// GetUpdate returns the latest PR or Issue update for a GitHub repo URL.
func (c *Client) GetUpdate(ctx context.Context, url string) (*pkgdomain.UpdateInfo, error) {
	owner, repo, err := parseGitHubOwnerRepo(url)
	if err != nil {
		return nil, err
	}

	item, err := c.fetchLatestIssueOrPR(ctx, owner, repo)
	if err != nil {
		return nil, err
	}
	if item == nil {
		repoUpdated, repoErr := c.fetchRepoUpdatedAt(ctx, owner, repo)
		if repoErr != nil {
			return nil, repoErr
		}
		return &pkgdomain.UpdateInfo{UpdatedAt: repoUpdated}, nil
	}

	return &pkgdomain.UpdateInfo{
		UpdatedAt: item.UpdatedAt,
		Title:     item.Title,
		Author:    item.User.Login,
		CreatedAt: item.CreatedAt,
		Preview:   item.Body,
	}, nil
}

// GetLastUpdated returns last updated time — kept for interface compatibility.
func (c *Client) GetLastUpdated(ctx context.Context, url string) (time.Time, error) {
	info, err := c.GetUpdate(ctx, url)
	if err != nil {
		return time.Time{}, err
	}
	return info.UpdatedAt, nil
}

// Supports returns true if this client can handle the given URL.
func (c *Client) Supports(url string) bool {
	return strings.HasPrefix(url, "https://github.com")
}

func (c *Client) fetchLatestIssueOrPR(ctx context.Context, owner, repo string) (*issueOrPRResponse, error) {
	apiURL := fmt.Sprintf("%s/repos/%s/%s/issues?state=all&sort=updated&direction=desc&per_page=1", c.baseURL, owner, repo)

	req, err := c.newRequest(ctx, apiURL)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req) //nolint:gosec // baseURL is trusted or hardcoded
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best effort to close body

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("github api status: %d", resp.StatusCode)
	}

	var items []issueOrPRResponse
	if err = json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(items) == 0 {
		//nolint:nilnil // no updates found is a valid business case handled by the caller
		return nil, nil
	}
	return &items[0], nil
}

type repoResponse struct {
	UpdatedAt time.Time `json:"updated_at"`
}

func (c *Client) fetchRepoUpdatedAt(ctx context.Context, owner, repo string) (time.Time, error) {
	apiURL := fmt.Sprintf("%s/repos/%s/%s", c.baseURL, owner, repo)

	req, err := c.newRequest(ctx, apiURL)
	if err != nil {
		return time.Time{}, err
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
		return time.Time{}, fmt.Errorf("github api status: %d", resp.StatusCode)
	}

	var result repoResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return time.Time{}, fmt.Errorf("decode response: %w", err)
	}

	return result.UpdatedAt, nil
}

func (c *Client) newRequest(ctx context.Context, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

func parseGitHubOwnerRepo(url string) (owner, repo string, err error) {
	trimmed := strings.TrimPrefix(url, "https://github.com")
	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	const minPathParts = 2
	if len(parts) < minPathParts || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid github url: %s", url)
	}
	return parts[0], parts[1], nil
}
