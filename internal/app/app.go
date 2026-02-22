package app

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application/handlers"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/config"
	memory "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/telegram"
)

type App struct {
	logger *slog.Logger
	cfg    *config.Config
}

func New(logger *slog.Logger, cfg *config.Config) *App {
	return &App{logger: logger, cfg: cfg}
}

func (a *App) Run() error {
	api, err := tgbotapi.NewBotAPI(a.cfg.TelegramToken)
	if err != nil {
		return fmt.Errorf("create bot api: %w", err)
	}

	a.logger.Info("bot authorized", slog.String("username", api.Self.UserName))

	userRepo := memory.NewUserRepository()

	handlerList := []application.CommandHandler{
		handlers.NewStartHandler(userRepo),
		handlers.NewHelpHandler(),
	}

	svc := application.NewBotService(handlerList)

	metas := make([]models.CommandMeta, len(handlerList))
	for i, h := range handlerList {
		metas[i] = h.Meta()
	}

	registry := telegram.NewRegistry(api)
	if err := registry.Register(context.Background(), metas); err != nil {
		a.logger.Warn("failed to register commands", slog.String("error", err.Error()))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	bot := telegram.NewBot(api, svc, a.logger)

	a.logger.Info("bot is starting")
	bot.Start(ctx)
	a.logger.Info("bot stopped")

	return nil
}
