package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/memory"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
)

type mockChecker struct {
	url     string
	updated time.Time
}

func (m *mockChecker) Supports(url string) bool { return url == m.url }
func (m *mockChecker) GetUpdate(_ context.Context, _ string) (*pkgdomain.UpdateInfo, error) {
	return &pkgdomain.UpdateInfo{
		UpdatedAt: m.updated,
		Title:     "Test title",
		Author:    "testuser",
		CreatedAt: m.updated,
		Preview:   "Test preview",
	}, nil
}

type mockNotifier struct {
	updates []pkgdomain.LinkUpdate
}

func (m *mockNotifier) SendUpdate(_ context.Context, u pkgdomain.LinkUpdate) error {
	m.updates = append(m.updates, u)
	return nil
}

func TestCheckUpdates_NotifiesOnlySubscribedChats(t *testing.T) {
	chatRepo := memory.NewChatRepository()
	linkRepo := memory.NewLinkRepository()
	subsRepo := memory.NewSubscriptionRepository(linkRepo)
	tagRepo := memory.NewTagRepository()
	notifier := &mockNotifier{}

	url := "https://github.com/user/repo"
	checker := &mockChecker{url: url, updated: time.Now().Add(time.Second)}

	svc := service.NewScrapperService(chatRepo, linkRepo, subsRepo, tagRepo, []service.LinkChecker{checker}, notifier, 4, 10)
	ctx := context.Background()

	require.NoError(t, svc.RegisterChat(ctx, 1))
	require.NoError(t, svc.RegisterChat(ctx, 2))

	_, err := svc.AddLink(ctx, 1, url, nil)
	require.NoError(t, err)

	require.NoError(t, svc.CheckUpdates(ctx))

	require.Len(t, notifier.updates, 1)
	assert.Equal(t, []int64{1}, notifier.updates[0].TgChatIDs)
}

func TestCheckUpdates_OtherChatsNotNotified(t *testing.T) {
	chatRepo := memory.NewChatRepository()
	linkRepo := memory.NewLinkRepository()
	subsRepo := memory.NewSubscriptionRepository(linkRepo)
	tagRepo := memory.NewTagRepository()
	notifier := &mockNotifier{}

	url := "https://github.com/user/repo"
	checker := &mockChecker{url: url, updated: time.Now().Add(time.Second)}

	svc := service.NewScrapperService(chatRepo, linkRepo, subsRepo, tagRepo, []service.LinkChecker{checker}, notifier, 4, 10)
	ctx := context.Background()

	require.NoError(t, svc.RegisterChat(ctx, 1))
	require.NoError(t, svc.RegisterChat(ctx, 2))
	_, err := svc.AddLink(ctx, 1, url, nil)
	require.NoError(t, err)

	require.NoError(t, svc.CheckUpdates(ctx))

	for _, u := range notifier.updates {
		for _, id := range u.TgChatIDs {
			assert.NotEqual(t, int64(2), id)
		}
	}
}

func TestCheckUpdates_DescriptionContainsRequiredFields(t *testing.T) {
	chatRepo := memory.NewChatRepository()
	linkRepo := memory.NewLinkRepository()
	subsRepo := memory.NewSubscriptionRepository(linkRepo)
	tagRepo := memory.NewTagRepository()
	notifier := &mockNotifier{}

	url := "https://github.com/user/repo"
	checker := &mockChecker{url: url, updated: time.Now().Add(time.Second)}

	svc := service.NewScrapperService(chatRepo, linkRepo, subsRepo, tagRepo, []service.LinkChecker{checker}, notifier, 1, 10)
	ctx := context.Background()

	require.NoError(t, svc.RegisterChat(ctx, 1))
	_, err := svc.AddLink(ctx, 1, url, nil)
	require.NoError(t, err)

	require.NoError(t, svc.CheckUpdates(ctx))

	require.Len(t, notifier.updates, 1)
	desc := notifier.updates[0].Description
	assert.Contains(t, desc, "Test title")
	assert.Contains(t, desc, "testuser")
	assert.Contains(t, desc, "Test preview")
}

func TestCheckUpdates_NoUpdateWhenNotChanged(t *testing.T) {
	chatRepo := memory.NewChatRepository()
	linkRepo := memory.NewLinkRepository()
	subsRepo := memory.NewSubscriptionRepository(linkRepo)
	tagRepo := memory.NewTagRepository()
	notifier := &mockNotifier{}

	url := "https://github.com/user/repo"
	now := time.Now().Truncate(time.Second)

	checker := &mockChecker{url: url, updated: now}

	svc := service.NewScrapperService(chatRepo, linkRepo, subsRepo, tagRepo, []service.LinkChecker{checker}, notifier, 1, 10)
	ctx := context.Background()

	require.NoError(t, svc.RegisterChat(ctx, 1))
	l, err := svc.AddLink(ctx, 1, url, nil)
	require.NoError(t, err)

	err = linkRepo.UpdateTime(ctx, l.ID, now)
	require.NoError(t, err)

	require.NoError(t, svc.CheckUpdates(ctx))

	assert.Empty(t, notifier.updates)
}
