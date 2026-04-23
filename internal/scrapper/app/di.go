package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/migrate"
	scrappergrpc "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/in/grpc"
	httphandler "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/in/http"
	botclient "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/bot"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/github"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/stackoverflow"
	kafkanotifier "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	memory "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/memory"
	pgxrepo "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/pgx"
	squirrelrepo "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/squirrel"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/scheduler"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
)

// Container holds all scrapper dependencies.
type Container struct {
	Server     *httphandler.Server
	GRPCServer *scrappergrpc.Server
	Scheduler  *scheduler.Scheduler
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

	chatRepo, linkRepo, subscriptionRepo, tagRepo, transactor := buildRepos(cfg, db)

	var notifier service.BotNotifier
	switch cfg.NotifierType {
	case NotifierKafka:
		producer := kafkanotifier.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic)
		notifier = producer
		logger.Info("using kafka notifier", slog.String("topic", cfg.Kafka.Topic))
	case NotifierHTTP:
		notifier, err = botclient.NewClient(cfg.BotURL)
		if err != nil {
			return nil, fmt.Errorf("create bot client: %w", err)
		}
		logger.Info("using http notifier", slog.String("bot_url", cfg.BotURL))
	}

	checkers := []service.LinkChecker{
		github.NewClient(cfg.GitHubToken),
		stackoverflow.NewClient(),
	}

	svc := service.NewScrapperService(
		chatRepo,
		linkRepo,
		subscriptionRepo,
		tagRepo,
		checkers,
		notifier,
		cfg.WorkerCount,
		cfg.BatchSize,
		transactor,
	)

	handler := httphandler.NewHandler(svc, logger)
	server := httphandler.NewServer(":"+cfg.Port, handler)

	sched, err := scheduler.NewScheduler(svc, logger, cfg.ScheduleInterval)
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}

	grpcServer := scrappergrpc.NewServer(svc)
	return &Container{
		Server:     server,
		GRPCServer: grpcServer,
		Scheduler:  sched,
	}, nil
}

func buildRepos(cfg *Config, db *pgxpool.Pool) (
	service.ChatRepository,
	service.LinkRepository,
	service.SubscriptionRepository,
	service.TagRepository,
	service.Transactor,
) {
	switch cfg.AccessType {
	case "squirrel":
		return squirrelrepo.NewChatRepository(db),
			squirrelrepo.NewLinkRepository(db),
			squirrelrepo.NewSubscriptionRepository(db),
			squirrelrepo.NewTagRepository(db),
			repository.NewTransactor(db)
	case "pgx":
		return pgxrepo.NewChatRepository(db),
			pgxrepo.NewLinkRepository(db),
			pgxrepo.NewSubscriptionRepository(db),
			pgxrepo.NewTagRepository(db),
			repository.NewTransactor(db)
	default:
		memLinkRepo := memory.NewLinkRepository()
		return memory.NewChatRepository(),
			memLinkRepo,
			memory.NewSubscriptionRepository(memLinkRepo),
			memory.NewTagRepository(),
			memory.NewTransactor()
	}
}
