// Package di provides dependency injection container.
package di

import (
	"fmt"
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application/handlers"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/memory"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/telegram"
)

// Container holds all application dependencies.
type Container struct {
	Bot      *telegram.Bot
	Registry *telegram.Registry
	Metas    []domain.CommandMeta
}

// New builds and wires all dependencies.
func New(cfg *config.Config, logger *slog.Logger) (*Container, error) {
	tgClient, err := tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		return nil, fmt.Errorf("create tg client: %w", err)
	}

	userRepo := memory.NewUserRepository()

	handlerList := []application.CommandHandler{
		handlers.NewStartHandler(userRepo),
		handlers.NewHelpHandler(),
	}

	svc := application.NewBotService(handlerList)

	metas := make([]domain.CommandMeta, len(handlerList))
	for i, h := range handlerList {
		metas[i] = h.Meta()
	}

	return &Container{
		Bot:      telegram.NewBot(tgClient, svc, logger),
		Registry: telegram.NewRegistry(tgClient),
		Metas:    metas,
	}, nil
}
