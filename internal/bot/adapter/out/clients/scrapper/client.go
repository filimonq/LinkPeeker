// Package scrapper provides HTTP client for scrapper service.
package scrapper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
)

const defaultTimeout = 10 * time.Second

// Client is an HTTP client for scrapper service.
type Client struct {
	http    *http.Client
	baseURL string
}

// NewClient creates a new scrapper Client.
func NewClient(baseURL string) (*Client, error) {
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("invalid base url: %w", err)
	}
	return &Client{
		http:    &http.Client{Timeout: defaultTimeout},
		baseURL: baseURL,
	}, nil
}

type addLinkRequest struct {
	Link string   `json:"link"`
	Tags []string `json:"tags"`
}

type removeLinkRequest struct {
	Link string `json:"link"`
}

type linkResponse struct {
	ID   int64    `json:"id"`
	URL  string   `json:"url"`
	Tags []string `json:"tags"`
}

type listLinksResponse struct {
	Links []linkResponse `json:"links"`
}

// RegisterChat registers a chat in scrapper.
func (c *Client) RegisterChat(ctx context.Context, chatID int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/tg-chat/%d", c.baseURL, chatID), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.http.Do(req) //nolint:gosec // baseURL is validated in constructor
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusConflict {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("scrapper status: %d", resp.StatusCode)
	}
	return nil
}

// DeleteChat deletes a chat from scrapper.
func (c *Client) DeleteChat(ctx context.Context, chatID int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("%s/tg-chat/%d", c.baseURL, chatID), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.http.Do(req) //nolint:gosec // baseURL is validated in constructor
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("scrapper status: %d", resp.StatusCode)
	}
	return nil
}

// AddLink adds a link to scrapper.
func (c *Client) AddLink(ctx context.Context, chatID int64, rawURL string, tags []string) (*service.LinkInfo, error) {
	body, err := json.Marshal(addLinkRequest{Link: rawURL, Tags: tags})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/links", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tg-Chat-Id", strconv.FormatInt(chatID, 10))

	resp, err := c.http.Do(req) //nolint:gosec // baseURL is validated in constructor
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusConflict {
		return nil, service.ErrLinkAlreadyExists
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, service.ErrLinkNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrapper status: %d", resp.StatusCode)
	}

	var result linkResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &service.LinkInfo{ID: result.ID, URL: result.URL, Tags: result.Tags}, nil
}

// RemoveLink removes a link from scrapper.
func (c *Client) RemoveLink(ctx context.Context, chatID int64, rawURL string) (*service.LinkInfo, error) {
	body, err := json.Marshal(removeLinkRequest{Link: rawURL})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		c.baseURL+"/links", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tg-Chat-Id", strconv.FormatInt(chatID, 10))

	resp, err := c.http.Do(req) //nolint:gosec // baseURL is validated in constructor
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, service.ErrLinkNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrapper status: %d", resp.StatusCode)
	}

	var result linkResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &service.LinkInfo{ID: result.ID, URL: result.URL, Tags: result.Tags}, nil
}

// ListLinks returns all tracked links for a chat.
func (c *Client) ListLinks(ctx context.Context, chatID int64) ([]*service.LinkInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/links", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Tg-Chat-Id", strconv.FormatInt(chatID, 10))

	resp, err := c.http.Do(req) //nolint:gosec // baseURL is validated in constructor
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrapper status: %d", resp.StatusCode)
	}

	var result listLinksResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	links := make([]*service.LinkInfo, len(result.Links))
	for i, l := range result.Links {
		links[i] = &service.LinkInfo{ID: l.ID, URL: l.URL, Tags: l.Tags}
	}
	return links, nil
}
