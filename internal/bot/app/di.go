package app

import (
	"fmt"
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	bothttp "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/http"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/telegram"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/telegram/handlers"
	scrapperclient "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/out/clients/scrapper"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/out/repository/memory"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service/state"
)

// Container holds all application dependencies.
type Container struct {
	Bot        *telegram.Bot
	Registry   *telegram.Registry
	Metas      []domain.CommandMeta
	HTTPServer *bothttp.Server
}

// NewContainer builds and wires all dependencies.
func NewContainer(cfg *Config, logger *slog.Logger) (*Container, error) {
	var (
		tgClient *tgbotapi.BotAPI
		err      error
	)
	if cfg.TelegramAPIURL != "" {
		tgClient, err = tgbotapi.NewBotAPIWithAPIEndpoint(cfg.TelegramToken, cfg.TelegramAPIURL+"/bot%s/%s")
	} else {
		tgClient, err = tgbotapi.NewBotAPI(cfg.TelegramToken)
	}
	if err != nil {
		return nil, fmt.Errorf("create tg client: %w", err)
	}

	scrapperClient, err := scrapperclient.NewClient(cfg.ScrapperURL)
	if err != nil {
		return nil, fmt.Errorf("create scrapper client: %w", err)
	}

	userRepo := memory.NewUserRepository()
	storage := state.NewStorage()

	handlerList := []service.CommandHandler{
		handlers.NewStartHandler(userRepo, scrapperClient),
		handlers.NewHelpHandler(),
		handlers.NewTrackHandler(storage),
		handlers.NewUntrackHandler(storage),
		handlers.NewListHandler(scrapperClient),
		handlers.NewCancelHandler(storage),
		handlers.NewMessageHandler(scrapperClient, storage),
	}

	svc := service.NewBotService(handlerList, storage)

	var metas []domain.CommandMeta
	for _, h := range handlerList {
		if m := h.Meta(); m.Name != "" {
			metas = append(metas, m)
		}
	}

	bot := telegram.NewBot(tgClient, svc, logger)

	updateSvc := service.NewUpdateService(bot)
	httpServer := bothttp.NewServer(":"+cfg.Port, bothttp.NewHandler(updateSvc, logger))

	return &Container{
		Bot:        bot,
		Registry:   telegram.NewRegistry(tgClient),
		Metas:      metas,
		HTTPServer: httpServer,
	}, nil
}
