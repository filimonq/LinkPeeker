// Package cache provides a Valkey-backed implementation of LinkCache.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

const keyPrefix = "links:"

type linkDTO struct {
	ID        int64     `json:"id"`
	URL       string    `json:"url"`
	UpdatedAt time.Time `json:"updated_at"`
	Tags      []string  `json:"tags"`
}

// ValkeyCache implements service.LinkCache using a Valkey cluster
// with client-side caching enabled via RESP3.
type ValkeyCache struct {
	client   valkey.Client
	ttl      time.Duration
	localTTL time.Duration
}

// New creates a new ValkeyCache with client-side caching enabled.
func New(addrs []string, password string, ttl time.Duration) (*ValkeyCache, error) {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  addrs,
		Password:     password,
		DisableCache: false,
	})
	if err != nil {
		return nil, fmt.Errorf("create valkey client: %w", err)
	}

	return &ValkeyCache{
		client:   client,
		ttl:      ttl,
		localTTL: ttl / 2,
	}, nil
}

// Close closes the underlying client.
func (c *ValkeyCache) Close() {
	c.client.Close()
}

// Get returns cached links for chatID, or (nil, nil) on cache miss.
func (c *ValkeyCache) Get(ctx context.Context, chatID int64) ([]*link.WithTags, error) {
	key := cacheKey(chatID)

	resp := c.client.DoCache(ctx,
		c.client.B().Get().Key(key).Cache(),
		c.localTTL,
	)

	raw, err := resp.ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, nil // cache miss
		}
		return nil, fmt.Errorf("valkey get: %w", err)
	}

	var dtos []linkDTO
	if err = json.Unmarshal([]byte(raw), &dtos); err != nil {
		return nil, nil
	}

	links := make([]*link.WithTags, len(dtos))
	for i, d := range dtos {
		links[i] = &link.WithTags{
			Link: &link.Link{
				ID:        link.ID(d.ID),
				URL:       d.URL,
				UpdatedAt: d.UpdatedAt,
			},
			Tags: d.Tags,
		}
	}
	return links, nil
}

// Set stores links for chatID with the configured TTL.
func (c *ValkeyCache) Set(ctx context.Context, chatID int64, links []*link.WithTags) error {
	dtos := make([]linkDTO, len(links))
	for i, l := range links {
		dtos[i] = linkDTO{
			ID:        int64(l.ID),
			URL:       l.URL,
			UpdatedAt: l.UpdatedAt,
			Tags:      l.Tags,
		}
	}

	payload, err := json.Marshal(dtos)
	if err != nil {
		return fmt.Errorf("marshal links: %w", err)
	}

	key := cacheKey(chatID)
	cmd := c.client.B().Set().Key(key).Value(string(payload)).
		ExSeconds(int64(c.ttl.Seconds())).Build()

	if err = c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey set: %w", err)
	}
	return nil
}

// Invalidate removes the cached entry for chatID.
func (c *ValkeyCache) Invalidate(ctx context.Context, chatID int64) error {
	key := cacheKey(chatID)
	err := c.client.Do(ctx, c.client.B().Del().Key(key).Build()).Error()
	if err != nil && !errors.Is(err, valkey.Nil) {
		return fmt.Errorf("valkey del: %w", err)
	}
	return nil
}

func cacheKey(chatID int64) string {
	return fmt.Sprintf("%s%d", keyPrefix, chatID)
}
