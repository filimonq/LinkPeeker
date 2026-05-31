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
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/middleware"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/migrate"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/observability"
)

type Container struct {
	Bot           *telegram.Bot
	Registry      *telegram.Registry
	Metas         []domain.CommandMeta
	HTTPServer    *bothttp.Server
	MetricsServer *observability.MetricsServer
	KafkaConsumer *kafkaconsumer.Consumer
}

func NewContainer(cfg *Config, logger *slog.Logger) (*Container, error) {
	metrics := observability.NewBotMetrics()
	userRepo, err := buildUserRepo(cfg)
	if err != nil {
		return nil, err
	}

	tgClient, err := buildTelegramClient(cfg)
	if err != nil {
		return nil, err
	}

	scrapperClient, err := buildScrapperClient(cfg, metrics)
	if err != nil {
		return nil, err
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

	bot := telegram.NewBot(tgClient, svc, logger, metrics)

	updateSvc := service.NewUpdateService(bot)
	updateSvc = service.InstrumentUpdateService(updateSvc, metrics)

	httpHandler := bothttp.NewHandler(updateSvc, logger)
	httpServer := bothttp.NewServer(
		":"+cfg.Port,
		httpHandler,
		metrics.HTTPMiddleware(),
		middleware.PerIPRateLimiter(cfg.RateLimit.RPS, cfg.RateLimit.Burst),
	)
	metricsServer := observability.NewMetricsServer(":"+cfg.MetricsPort, metrics.Registry)
	consumer := kafkaconsumer.NewConsumer(
		cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID,
		cfg.Kafka.DLQTopic, cfg.Kafka.MaxRetries,
		updateSvc, logger,
	)

	return &Container{
		Bot:           bot,
		Registry:      telegram.NewRegistry(tgClient),
		Metas:         metas,
		HTTPServer:    httpServer,
		MetricsServer: metricsServer,
		KafkaConsumer: consumer,
	}, nil
}

func buildUserRepo(cfg *Config) (service.UserRepository, error) {
	db, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}
	if err = migrate.Run(cfg.DatabaseURL, "database"); err != nil {
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	switch cfg.AccessType {
	case "squirrel":
		return squirrelrepo.NewUserRepository(db), nil
	case "pgx":
		return pgxrepo.NewUserRepository(db), nil
	default:
		return memory.NewUserRepository(), nil
	}
}

func buildTelegramClient(cfg *Config) (*tgbotapi.BotAPI, error) {
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

	return tgClient, nil
}

func buildScrapperClient(cfg *Config, metrics *observability.BotMetrics) (service.ScrapperClient, error) {
	rawScrapperClient, err := scrapperclient.NewClient(cfg.ScrapperURL)
	if err != nil {
		return nil, fmt.Errorf("create scrapper client: %w", err)
	}

	var scrapperClient service.ScrapperClient = rawScrapperClient
	return service.InstrumentScrapperClient(scrapperClient, metrics), nil
}
