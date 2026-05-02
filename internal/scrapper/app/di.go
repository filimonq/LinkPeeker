package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/migrate"
	scrappergrpc "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/in/grpc"
	httphandler "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/in/http"
	valkeycache "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/cache"
	botclient "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/bot"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/github"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/stackoverflow"
	kafkanotifier "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/outbox"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	memory "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/memory"
	pgxrepo "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/pgx"
	squirrelrepo "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/squirrel"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/scheduler"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
)

// Container holds all scrapper dependencies.
type Container struct {
	Server       *httphandler.Server
	GRPCServer   *scrappergrpc.Server
	Scheduler    *scheduler.Scheduler
	OutboxPoller *outbox.Poller
	Cache        *valkeycache.ValkeyCache
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
	producer := kafkanotifier.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic)

	valkeyCache, err := valkeycache.New(cfg.Valkey.Addrs, cfg.Valkey.Password, cfg.Valkey.TTL)
	if err != nil {
		return nil, fmt.Errorf("create valkey cache: %w", err)
	}
	logger.Info("valkey cache connected",
		slog.Any("addrs", cfg.Valkey.Addrs),
		slog.Duration("ttl", cfg.Valkey.TTL),
	)

	var notifier service.BotNotifier
	var outboxPoller *outbox.Poller

	switch cfg.NotifierType {
	case NotifierKafkaOutbox:
		outboxRepo := buildOutboxRepo(cfg, db)
		outboxPoller = outbox.NewPoller(outboxRepo, producer, cfg.OutboxPollInterval, cfg.BatchSize, logger)
		logger.Info("using transactional outbox notifier", slog.String("topic", cfg.Kafka.Topic))

		svc := service.NewScrapperService(
			chatRepo, linkRepo, subscriptionRepo, tagRepo,
			buildCheckers(cfg), producer,
			cfg.WorkerCount, cfg.BatchSize, transactor,
		)
		service.WithOutbox(svc, outboxRepo)
		service.WithCache(svc, valkeyCache)
		return buildContainer(cfg, logger, svc, outboxPoller, valkeyCache)

	case NotifierKafka:
		notifier = producer
		logger.Info("using kafka notifier", slog.String("topic", cfg.Kafka.Topic))

	case NotifierHTTP:
		notifier, err = botclient.NewClient(cfg.BotURL)
		if err != nil {
			return nil, fmt.Errorf("create bot client: %w", err)
		}
		logger.Info("using http notifier", slog.String("bot_url", cfg.BotURL))
	}

	svc := service.NewScrapperService(
		chatRepo, linkRepo, subscriptionRepo, tagRepo,
		buildCheckers(cfg), notifier,
		cfg.WorkerCount, cfg.BatchSize, transactor,
	)
	service.WithCache(svc, valkeyCache)
	return buildContainer(cfg, logger, svc, nil, valkeyCache)
}

func buildOutboxRepo(cfg *Config, db *pgxpool.Pool) service.OutboxRepository {
	if cfg.AccessType == "squirrel" {
		return squirrelrepo.NewOutboxRepository(db)
	}
	return pgxrepo.NewOutboxRepository(db)
}

func buildCheckers(cfg *Config) []service.LinkChecker {
	return []service.LinkChecker{
		github.NewClient(cfg.GitHubToken),
		stackoverflow.NewClient(),
	}
}

func buildContainer(cfg *Config, logger *slog.Logger, svc service.ScrapperService, outboxPoller *outbox.Poller, cache *valkeycache.ValkeyCache) (*Container, error) {
	handler := httphandler.NewHandler(svc, logger)
	server := httphandler.NewServer(":"+cfg.Port, handler)

	sched, err := scheduler.NewScheduler(svc, logger, cfg.ScheduleInterval)
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}

	grpcServer := scrappergrpc.NewServer(svc)
	return &Container{
		Server:       server,
		GRPCServer:   grpcServer,
		Scheduler:    sched,
		OutboxPoller: outboxPoller,
		Cache:        cache,
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
