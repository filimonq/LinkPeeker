package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/telegram/handlers"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/out/repository/memory"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/user"
)

func newSvc() BotService {
	userRepo := memory.NewUserRepository()
	return NewBotService([]CommandHandler{
		handlers.NewStartHandler(userRepo),
		handlers.NewHelpHandler(),
	})
}

func TestBotService_Handle(t *testing.T) {
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

			assert.NoError(t, err)
			assert.Contains(t, resp.Text, tt.expectedText)
			assert.Equal(t, tt.cmd.ChatID, resp.ChatID)
		})
	}
}

func TestBotService_Start_ExistingUser(t *testing.T) {
	userRepo := memory.NewUserRepository()
	svc := NewBotService([]CommandHandler{
		handlers.NewStartHandler(userRepo),
	})

	cmd := domain.Command{Type: domain.CommandStart, ChatID: 123, From: user.ID(99)}

	_, err := svc.HandleCommand(context.Background(), cmd)
	assert.NoError(t, err)

	// второй вызов
	resp, err := svc.HandleCommand(context.Background(), cmd)
	assert.NoError(t, err)
	assert.Contains(t, resp.Text, "С возвращением")
}
