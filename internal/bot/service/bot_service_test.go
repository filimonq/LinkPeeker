package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/telegram/handlers"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/out/repository/memory"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/botuser"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service/state"
)

type mockScrapperWithLinks struct {
	links []*service.LinkInfo
}

func (m *mockScrapperWithLinks) RegisterChat(_ context.Context, _ int64) error { return nil }
func (m *mockScrapperWithLinks) DeleteChat(_ context.Context, _ int64) error   { return nil }
func (m *mockScrapperWithLinks) AddLink(_ context.Context, _ int64, url string, tags []string) (*service.LinkInfo, error) {
	for _, l := range m.links {
		if l.URL == url {
			return nil, service.ErrLinkAlreadyExists
		}
	}
	l := &service.LinkInfo{URL: url, Tags: tags}
	m.links = append(m.links, l)
	return l, nil
}
func (m *mockScrapperWithLinks) RemoveLink(_ context.Context, _ int64, _ string) (*service.LinkInfo, error) {
	return &service.LinkInfo{}, nil
}
func (m *mockScrapperWithLinks) ListLinks(_ context.Context, _ int64) ([]*service.LinkInfo, error) {
	return m.links, nil
}

func newSvc() service.BotService {
	userRepo := memory.NewUserRepository()
	return service.NewBotService([]service.CommandHandler{
		handlers.NewStartHandler(userRepo, &mockScrapperWithLinks{}),
		handlers.NewHelpHandler(),
	}, state.NewStorage())
}

func newFullSvc(scrapper service.ScrapperClient) service.BotService {
	userRepo := memory.NewUserRepository()
	storage := state.NewStorage()
	return service.NewBotService([]service.CommandHandler{
		handlers.NewStartHandler(userRepo, scrapper),
		handlers.NewTrackHandler(storage),
		handlers.NewUntrackHandler(storage),
		handlers.NewListHandler(scrapper),
		handlers.NewCancelHandler(storage),
		handlers.NewMessageHandler(scrapper, storage),
	}, storage)
}

func TestBotService_Handle(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		cmd          domain.Command
		expectedText string
	}{
		"start — новый пользователь": {
			cmd:          domain.Command{Type: domain.CommandStart, ChatID: 123, From: 1},
			expectedText: "Добро пожаловать",
		},
		"help": {
			cmd:          domain.Command{Type: domain.CommandHelp, ChatID: 123},
			expectedText: "Доступные команды",
		},
		"неизвестная команда": {
			cmd:          domain.Command{Type: domain.CommandUnknown, ChatID: 123},
			expectedText: "Неизвестная команда",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newSvc()
			resp, err := svc.HandleCommand(context.Background(), tt.cmd)

			require.NoError(t, err)
			assert.Contains(t, resp.Text, tt.expectedText)
			assert.Equal(t, tt.cmd.ChatID, resp.ChatID)
		})
	}
}

func TestBotService_Start_ExistingUser(t *testing.T) {
	userRepo := memory.NewUserRepository()
	svc := service.NewBotService([]service.CommandHandler{
		handlers.NewStartHandler(userRepo, &mockScrapperWithLinks{}),
	}, state.NewStorage())

	cmd := domain.Command{Type: domain.CommandStart, ChatID: 123, From: botuser.ID(99)}

	_, err := svc.HandleCommand(context.Background(), cmd)
	require.NoError(t, err)

	// второй вызов
	resp, err := svc.HandleCommand(context.Background(), cmd)
	require.NoError(t, err)
	assert.Contains(t, resp.Text, "С возвращением")
}

func TestTrack_ValidURL_SavesLink(t *testing.T) {
	sc := &mockScrapperWithLinks{}
	svc := newFullSvc(sc)
	ctx := context.Background()
	chatID := int64(1)

	_, err := svc.HandleCommand(ctx, domain.Command{Type: domain.CommandTrack, ChatID: chatID})
	require.NoError(t, err)

	_, err = svc.HandleCommand(ctx, domain.Command{Type: domain.CommandMessage, ChatID: chatID, Payload: "https://github.com/user/repo"})
	require.NoError(t, err)

	resp, err := svc.HandleCommand(ctx, domain.Command{Type: domain.CommandMessage, ChatID: chatID, Payload: "go, backend"})
	require.NoError(t, err)
	assert.Contains(t, resp.Text, "добавлена")
	assert.Len(t, sc.links, 1)
	assert.Equal(t, "https://github.com/user/repo", sc.links[0].URL)
}

func TestTrack_InvalidURL_ReturnsError(t *testing.T) {
	svc := newFullSvc(&mockScrapperWithLinks{})
	ctx := context.Background()
	chatID := int64(1)

	_, err := svc.HandleCommand(ctx, domain.Command{Type: domain.CommandTrack, ChatID: chatID})
	require.NoError(t, err)

	resp, err := svc.HandleCommand(ctx, domain.Command{Type: domain.CommandMessage, ChatID: chatID, Payload: "tbank://github.com/user/repo"})
	require.NoError(t, err)
	assert.Contains(t, resp.Text, "Некорректная")
}

func TestTrack_DuplicateURL_ReturnsAlreadyExists(t *testing.T) {
	sc := &mockScrapperWithLinks{}
	svc := newFullSvc(sc)
	ctx := context.Background()
	chatID := int64(1)

	_, _ = svc.HandleCommand(ctx, domain.Command{Type: domain.CommandTrack, ChatID: chatID})
	_, _ = svc.HandleCommand(ctx, domain.Command{Type: domain.CommandMessage, ChatID: chatID, Payload: "https://github.com/user/repo"})
	_, _ = svc.HandleCommand(ctx, domain.Command{Type: domain.CommandMessage, ChatID: chatID, Payload: "go"})

	_, _ = svc.HandleCommand(ctx, domain.Command{Type: domain.CommandTrack, ChatID: chatID})
	_, _ = svc.HandleCommand(ctx, domain.Command{Type: domain.CommandMessage, ChatID: chatID, Payload: "https://github.com/user/repo"})
	resp, err := svc.HandleCommand(ctx, domain.Command{Type: domain.CommandMessage, ChatID: chatID, Payload: "go"})
	require.NoError(t, err)
	assert.Contains(t, resp.Text, "уже отслеживается")
}

func TestList_WithLinks_ReturnsList(t *testing.T) {
	sc := &mockScrapperWithLinks{
		links: []*service.LinkInfo{
			{URL: "https://github.com/user/repo", Tags: []string{"go"}},
		},
	}
	svc := newFullSvc(sc)

	resp, err := svc.HandleCommand(context.Background(), domain.Command{Type: domain.CommandList, ChatID: 1})
	require.NoError(t, err)
	assert.Contains(t, resp.Text, "https://github.com/user/repo")
}

func TestList_NoLinks_ReturnsEmpty(t *testing.T) {
	svc := newFullSvc(&mockScrapperWithLinks{})

	resp, err := svc.HandleCommand(context.Background(), domain.Command{Type: domain.CommandList, ChatID: 1})
	require.NoError(t, err)
	assert.Contains(t, resp.Text, "ничего не отслеживаете")
}

func TestList_WithTag_FiltersLinks(t *testing.T) {
	sc := &mockScrapperWithLinks{
		links: []*service.LinkInfo{
			{URL: "https://github.com/user/repo1", Tags: []string{"go"}},
			{URL: "https://github.com/user/repo2", Tags: []string{"python"}},
		},
	}
	svc := newFullSvc(sc)

	resp, err := svc.HandleCommand(context.Background(), domain.Command{
		Type: domain.CommandList, ChatID: 1, Payload: "go",
	})
	require.NoError(t, err)
	assert.Contains(t, resp.Text, "repo1")
	assert.NotContains(t, resp.Text, "repo2")
}
