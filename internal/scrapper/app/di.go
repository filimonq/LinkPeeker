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

	var (
		chatRepo         service.ChatRepository
		linkRepo         service.LinkRepository
		subscriptionRepo service.SubscriptionRepository
		tagRepo          service.TagRepository
		transactor       service.Transactor
	)
	switch cfg.AccessType {
	case "squirrel":
		chatRepo = squirrelrepo.NewChatRepository(db)
		linkRepo = squirrelrepo.NewLinkRepository(db)
		subscriptionRepo = squirrelrepo.NewSubscriptionRepository(db)
		tagRepo = squirrelrepo.NewTagRepository(db)
		transactor = repository.NewTransactor(db)
	case "pgx":
		chatRepo = pgxrepo.NewChatRepository(db)
		linkRepo = pgxrepo.NewLinkRepository(db)
		subscriptionRepo = pgxrepo.NewSubscriptionRepository(db)
		tagRepo = pgxrepo.NewTagRepository(db)
		transactor = repository.NewTransactor(db)
	default:
		memLinkRepo := memory.NewLinkRepository()
		chatRepo = memory.NewChatRepository()
		linkRepo = memLinkRepo
		subscriptionRepo = memory.NewSubscriptionRepository(memLinkRepo)
		tagRepo = memory.NewTagRepository()
		transactor = memory.NewTransactor()
	}

	botClient, err := botclient.NewClient(cfg.BotURL)
	if err != nil {
		return nil, fmt.Errorf("create bot client: %w", err)
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
		botClient,
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
