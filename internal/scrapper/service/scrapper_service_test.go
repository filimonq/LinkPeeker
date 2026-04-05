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

// тут сценарий 7
// Планировщик отправляет обновление только пользователям, которые следят за ссылкой
// Другие пользователи не получают обновление

type mockChecker struct {
	url     string
	updated time.Time
}

func (m *mockChecker) Supports(url string) bool { return url == m.url }
func (m *mockChecker) GetLastUpdated(_ context.Context, _ string) (time.Time, error) {
	return m.updated, nil
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
