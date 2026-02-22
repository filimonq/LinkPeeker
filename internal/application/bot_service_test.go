package application_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application/handlers"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
	memory "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository"
)

func TestBotService_Handle(t *testing.T) {
	userRepo := memory.NewUserRepository()
	svc := application.NewBotService([]application.CommandHandler{
		handlers.NewStartHandler(userRepo),
		handlers.NewHelpHandler(),
	})

	tests := []struct {
		name         string
		cmd          models.Command
		expectedText string
	}{
		{
			name:         "start — новый пользователь",
			cmd:          models.Command{Type: models.CommandStart, ChatID: 123, From: 1},
			expectedText: "Добро пожаловать",
		},
		{
			name:         "start — существующий пользователь",
			cmd:          models.Command{Type: models.CommandStart, ChatID: 123, From: 1},
			expectedText: "С возвращением",
		},
		{
			name:         "help",
			cmd:          models.Command{Type: models.CommandHelp, ChatID: 123},
			expectedText: "Доступные команды",
		},
		{
			name:         "неизвестная команда",
			cmd:          models.Command{Type: models.CommandUnknown, ChatID: 123},
			expectedText: "Неизвестная команда",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := svc.HandleCommand(context.Background(), tt.cmd)

			assert.NoError(t, err)
			assert.Contains(t, resp.Text, tt.expectedText)
			assert.Equal(t, tt.cmd.ChatID, resp.ChatID)
		})
	}
}
