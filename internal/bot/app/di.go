package app

import (
	"context"
	"fmt"
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	bothttp "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/http"
	kafkaconsumer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/telegram"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/telegram/handlers"
	scrapperclient "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/out/clients/scrapper"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/out/repository/memory"
	pgxrepo "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/out/repository/pgx"
	squirrelrepo "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/out/repository/squirrel"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service/state"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/migrate"
)

// Container holds all application dependencies.
type Container struct {
	Bot           *telegram.Bot
	Registry      *telegram.Registry
	Metas         []domain.CommandMeta
	HTTPServer    *bothttp.Server
	KafkaConsumer *kafkaconsumer.Consumer
}

// NewContainer builds and wires all dependencies.
func NewContainer(cfg *Config, logger *slog.Logger) (*Container, error) {
	db, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}

	if err = migrate.Run(cfg.DatabaseURL, "database"); err != nil {
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	var userRepo service.UserRepository
	switch cfg.AccessType {
	case "squirrel":
		userRepo = squirrelrepo.NewUserRepository(db)
	case "pgx":
		userRepo = pgxrepo.NewUserRepository(db)
	default:
		userRepo = memory.NewUserRepository()
	}

	var tgClient *tgbotapi.BotAPI
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

	consumer := kafkaconsumer.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
		cfg.Kafka.GroupID,
		updateSvc,
		logger,
	)

	return &Container{
		Bot:           bot,
		Registry:      telegram.NewRegistry(tgClient),
		Metas:         metas,
		HTTPServer:    httpServer,
		KafkaConsumer: consumer,
	}, nil
}
