// Package di provides dependency injection container.
package app

import (
	"fmt"
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/telegram"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/telegram/handlers"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/out/repository/memory"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

// Container holds all application dependencies.
type Container struct {
	Bot      *telegram.Bot
	Registry *telegram.Registry
	Metas    []domain.CommandMeta
}

// New builds and wires all dependencies.
func NewContainer(cfg *Config, logger *slog.Logger) (*Container, error) {
	tgClient, err := tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		return nil, fmt.Errorf("create tg client: %w", err)
	}

	userRepo := memory.NewUserRepository()

	handlerList := []service.CommandHandler{
		handlers.NewStartHandler(userRepo),
		handlers.NewHelpHandler(),
	}

	svc := service.NewBotService(handlerList)

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
