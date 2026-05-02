//go:build integration

package cache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	cache "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/cache"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

func startValkey(t *testing.T, ctx context.Context) string {
	t.Helper()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:9.0-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "6379")
	return fmt.Sprintf("%s:%s", host, port.Port())
}

func makeLinks() []*link.WithTags {
	l := &link.Link{}
	l.ID = 1
	l.URL = "https://github.com/user/repo"
	return []*link.WithTags{
		{Link: l, Tags: []string{"go", "backend"}},
	}
}

func TestValkeyCache_SetAndGet(t *testing.T) {
	ctx := context.Background()
	addr := startValkey(t, ctx)

	c, err := cache.New([]string{addr}, "", 10*time.Second)
	require.NoError(t, err)
	defer c.Close()

	links := makeLinks()
	require.NoError(t, c.Set(ctx, 42, links))

	got, err := c.Get(ctx, 42)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Len(t, got, 1)
	assert.Equal(t, links[0].URL, got[0].URL)
	assert.Equal(t, links[0].Tags, got[0].Tags)
}

func TestValkeyCache_Miss(t *testing.T) {
	ctx := context.Background()
	addr := startValkey(t, ctx)

	c, err := cache.New([]string{addr}, "", 10*time.Second)
	require.NoError(t, err)
	defer c.Close()

	got, err := c.Get(ctx, 999)
	require.NoError(t, err)
	assert.Nil(t, got, "ожидаем nil на cache miss")
}

func TestValkeyCache_Invalidate(t *testing.T) {
	ctx := context.Background()
	addr := startValkey(t, ctx)

	c, err := cache.New([]string{addr}, "", 10*time.Second)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, 7, makeLinks()))

	require.NoError(t, c.Invalidate(ctx, 7))

	got, err := c.Get(ctx, 7)
	require.NoError(t, err)
	assert.Nil(t, got, "после инвалидации должен быть cache miss")
}

func TestValkeyCache_TTLExpiry(t *testing.T) {
	ctx := context.Background()
	addr := startValkey(t, ctx)

	c, err := cache.New([]string{addr}, "", 1*time.Second)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, 5, makeLinks()))

	got, err := c.Get(ctx, 5)
	require.NoError(t, err)
	require.NotNil(t, got)

	time.Sleep(2 * time.Second)
	got, err = c.Get(ctx, 5)
	require.NoError(t, err)
	assert.Nil(t, got, "после TTL должен быть cache miss")
}

func TestValkeyCache_InvalidateNonExistent(t *testing.T) {
	ctx := context.Background()
	addr := startValkey(t, ctx)

	c, err := cache.New([]string{addr}, "", 10*time.Second)
	require.NoError(t, err)
	defer c.Close()

	assert.NoError(t, c.Invalidate(ctx, 12345))
}
