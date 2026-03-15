package app

import (
	"fmt"
	"log/slog"

	scrappergrpc "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/in/grpc"
	httphandler "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/in/http"
	botclient "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/bot"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/github"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/stackoverflow"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/memory"
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
	chatRepo := memory.NewChatRepository()
	linkRepo := memory.NewLinkRepository()

	botClient, err := botclient.NewClient(cfg.BotURL)
	if err != nil {
		return nil, fmt.Errorf("create bot client: %w", err)
	}

	checkers := []service.LinkChecker{
		github.NewClient(cfg.GitHubToken),
		stackoverflow.NewClient(),
	}

	svc := service.NewScrapperService(chatRepo, linkRepo, checkers, botClient)

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
