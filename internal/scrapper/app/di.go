package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/middleware"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/migrate"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/observability"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/resilience"
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
	metrics := observability.NewScrapperMetrics()
	db, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}

	if err = migrate.Run(cfg.DatabaseURL, "database"); err != nil {
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	chatRepo, linkRepo, subscriptionRepo, tagRepo, transactor := buildInstrumentedRepos(cfg, db, metrics)
	_, instrumentedProducer := buildKafkaNotifiers(cfg, metrics)

	valkeyCache, err := valkeycache.New(cfg.Valkey.Addrs, cfg.Valkey.Password, cfg.Valkey.TTL)
	if err != nil {
		return nil, fmt.Errorf("create valkey cache: %w", err)
	}

	checkers := buildCheckers(cfg, metrics)
	botRC := newResilientClient(cfg.Resilience, "bot-http")

	var notifier service.BotNotifier
	var outboxPoller *outbox.Poller

	switch cfg.NotifierType {
	case NotifierKafkaOutbox:
		outboxRepo := service.InstrumentOutboxRepository(buildOutboxRepo(cfg, db), metrics)
		outboxPoller = outbox.NewPoller(outboxRepo, instrumentedProducer, cfg.OutboxPollInterval, cfg.BatchSize, logger)
		logger.Info("using transactional outbox notifier", slog.String("topic", cfg.Kafka.Topic))

		svc := service.NewScrapperService(
			chatRepo, linkRepo, subscriptionRepo, tagRepo,
			checkers, instrumentedProducer,
			cfg.WorkerCount, cfg.BatchSize, transactor,
		)
		service.WithOutbox(svc, outboxRepo)
		service.WithCache(svc, valkeyCache)
		service.WithMetrics(svc, metrics)
		return buildContainer(cfg, logger, svc, outboxPoller, valkeyCache, metrics)

	case NotifierKafka:
		notifier = instrumentedProducer
		logger.Info("using kafka notifier")

	case NotifierHTTP:
		httpNotifier, clientErr := botclient.NewClient(cfg.BotURL, botRC)
		if clientErr != nil {
			return nil, fmt.Errorf("create bot client: %w", clientErr)
		}
		notifier = service.InstrumentBotNotifier(botclient.NewFallbackNotifier(httpNotifier, instrumentedProducer, logger), metrics, "http")
		logger.Info("using HTTP notifier with Kafka fallback", slog.String("bot_url", cfg.BotURL))
	}

	svc := service.NewScrapperService(
		chatRepo, linkRepo, subscriptionRepo, tagRepo,
		checkers, notifier,
		cfg.WorkerCount, cfg.BatchSize, transactor,
	)
	service.WithCache(svc, valkeyCache)
	service.WithMetrics(svc, metrics)
	return buildContainer(cfg, logger, svc, nil, valkeyCache, metrics)
}

func buildInstrumentedRepos(
	cfg *Config,
	db *pgxpool.Pool,
	metrics *observability.ScrapperMetrics,
) (
	service.ChatRepository,
	service.LinkRepository,
	service.SubscriptionRepository,
	service.TagRepository,
	service.Transactor,
) {
	chatRepo, linkRepo, subscriptionRepo, tagRepo, transactor := buildRepos(cfg, db)
	return service.InstrumentChatRepository(chatRepo, metrics),
		service.InstrumentLinkRepository(linkRepo, metrics),
		service.InstrumentSubscriptionRepository(subscriptionRepo, metrics),
		service.InstrumentTagRepository(tagRepo, metrics),
		service.InstrumentTransactor(transactor, metrics)
}

func buildKafkaNotifiers(
	cfg *Config,
	metrics *observability.ScrapperMetrics,
) (*kafkanotifier.Producer, service.BotNotifier) {
	producer := kafkanotifier.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	return producer, service.InstrumentBotNotifier(producer, metrics, "kafka")
}

func buildCheckers(cfg *Config, metrics *observability.ScrapperMetrics) []service.LinkChecker {
	githubRC := newResilientClient(cfg.Resilience, "github")
	soRC := newResilientClient(cfg.Resilience, "stackoverflow")

	return []service.LinkChecker{
		service.InstrumentLinkChecker(github.NewClient(cfg.GitHubToken, githubRC), metrics),
		service.InstrumentLinkChecker(stackoverflow.NewClient(soRC), metrics),
	}
}

func newResilientClient(cfg ResilienceConfig, name string) *resilience.Client {
	return resilience.NewClient(resilience.Config{
		Timeout:               cfg.Timeout,
		RetryAttempts:         cfg.RetryAttempts,
		RetryDelay:            cfg.RetryDelay,
		RetryableStatuses:     cfg.RetryableStatuses,
		CBName:                name,
		CBMinCalls:            cfg.CBMinCalls,
		CBFailureRate:         cfg.CBFailureRate,
		CBHalfOpenCalls:       cfg.CBHalfOpenCalls,
		CBWaitDuration:        cfg.CBWaitDuration,
		UseExponentialBackoff: cfg.UseExponentialBackoff,
	})
}

func buildOutboxRepo(cfg *Config, db *pgxpool.Pool) service.OutboxRepository {
	if cfg.AccessType == "squirrel" {
		return squirrelrepo.NewOutboxRepository(db)
	}
	return pgxrepo.NewOutboxRepository(db)
}

func buildContainer(
	cfg *Config,
	logger *slog.Logger,
	svc service.ScrapperService,
	outboxPoller *outbox.Poller,
	cache *valkeycache.ValkeyCache,
	metrics *observability.ScrapperMetrics,
) (*Container, error) {
	handler := httphandler.NewHandler(svc, logger)

	server := httphandler.NewServer(
		":"+cfg.Port,
		handler,
		metrics.Registry,
		metrics.HTTPMiddleware(),
		middleware.PerIPRateLimiter(cfg.Resilience.RateLimitRPS, cfg.Resilience.RateLimitBurst),
	)

	sched, err := scheduler.NewScheduler(svc, logger, cfg.ScheduleInterval)
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}

	grpcServer := scrappergrpc.NewServer(svc, metrics)
	return &Container{
		Server:       server,
		GRPCServer:   grpcServer,
		Scheduler:    sched,
		OutboxPoller: outboxPoller,
		Cache:        cache,
	}, nil
}

func buildRepos(cfg *Config, db *pgxpool.Pool) (
	service.ChatRepository, service.LinkRepository,
	service.SubscriptionRepository, service.TagRepository, service.Transactor,
) {
	switch cfg.AccessType {
	case "squirrel":
		return squirrelrepo.NewChatRepository(db), squirrelrepo.NewLinkRepository(db),
			squirrelrepo.NewSubscriptionRepository(db), squirrelrepo.NewTagRepository(db),
			repository.NewTransactor(db)
	case "pgx":
		return pgxrepo.NewChatRepository(db), pgxrepo.NewLinkRepository(db),
			pgxrepo.NewSubscriptionRepository(db), pgxrepo.NewTagRepository(db),
			repository.NewTransactor(db)
	default:
		memLinkRepo := memory.NewLinkRepository()
		return memory.NewChatRepository(), memLinkRepo,
			memory.NewSubscriptionRepository(memLinkRepo), memory.NewTagRepository(),
			memory.NewTransactor()
	}
}
