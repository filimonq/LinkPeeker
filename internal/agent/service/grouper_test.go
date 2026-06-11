package service_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/service"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type threadSafePublisher struct {
	mu      sync.Mutex
	updates []pkgdomain.LinkUpdate
}

func (p *threadSafePublisher) SendUpdate(_ context.Context, update pkgdomain.LinkUpdate) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.updates = append(p.updates, update)
	return nil
}

func (p *threadSafePublisher) snapshot() []pkgdomain.LinkUpdate {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]pkgdomain.LinkUpdate(nil), p.updates...)
}

func TestGrouper_GroupsMultipleUpdatesForChat(t *testing.T) {
	publisher := &threadSafePublisher{}
	grouper := service.NewGrouper(20*time.Millisecond, publisher, noopLogger())
	t.Cleanup(func() {
		_ = grouper.Close(context.Background())
	})

	require.NoError(t, grouper.SendUpdate(context.Background(), pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://example.com/1",
		Description: "critical bug fix",
		TgChatIDs:   []int64{1},
		Priority:    "HIGH",
	}))
	require.NoError(t, grouper.SendUpdate(context.Background(), pkgdomain.LinkUpdate{
		ID:          2,
		URL:         "https://example.com/2",
		Description: "minor docs update",
		TgChatIDs:   []int64{1},
		Priority:    "LOW",
	}))

	require.Eventually(t, func() bool {
		return len(publisher.snapshot()) == 1
	}, time.Second, 10*time.Millisecond)

	updates := publisher.snapshot()
	require.Len(t, updates, 1)
	assert.Equal(t, []int64{1}, updates[0].TgChatIDs)
	assert.Equal(t, "HIGH", updates[0].Priority)
	assert.Contains(t, updates[0].Description, "1. critical bug fix")
	assert.Contains(t, updates[0].Description, "2. minor docs update")
}

func TestGrouper_LeavesSingleUpdateUngrouped(t *testing.T) {
	publisher := &threadSafePublisher{}
	grouper := service.NewGrouper(20*time.Millisecond, publisher, noopLogger())
	t.Cleanup(func() {
		_ = grouper.Close(context.Background())
	})

	update := pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://example.com/1",
		Description: "single update",
		TgChatIDs:   []int64{1},
		Priority:    "MEDIUM",
	}

	require.NoError(t, grouper.SendUpdate(context.Background(), update))

	require.Eventually(t, func() bool {
		return len(publisher.snapshot()) == 1
	}, time.Second, 10*time.Millisecond)

	updates := publisher.snapshot()
	require.Len(t, updates, 1)
	assert.Equal(t, update.Description, updates[0].Description)
	assert.Equal(t, update.Priority, updates[0].Priority)
	assert.Equal(t, update.TgChatIDs, updates[0].TgChatIDs)
}
