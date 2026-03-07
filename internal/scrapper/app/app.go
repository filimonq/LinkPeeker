// Package app assembles and runs the scrapper application.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
)

// App is the main scrapper application container.
type App struct {
	logger *slog.Logger
	cfg    *Config
}

// New creates a new App instance.
func New(logger *slog.Logger, cfg *Config) *App {
	return &App{logger: logger, cfg: cfg}
}

// Run initializes and starts the scrapper application.
func (a *App) Run() error {
	container, err := NewContainer(a.cfg, a.logger)
	if err != nil {
		return fmt.Errorf("build container: %w", err)
	}

	if err := container.Scheduler.Start(); err != nil {
		return fmt.Errorf("start scheduler: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		a.logger.Info("scrapper is starting", slog.String("port", a.cfg.Port))
		if err := container.Server.Start(); err != nil {
			a.logger.Error("server error", slog.String("error", err.Error()))
		}
	}()

	<-ctx.Done()
	a.logger.Info("scrapper is shutting down")

	if err := container.Scheduler.Stop(); err != nil {
		a.logger.Error("scheduler stop error", slog.String("error", err.Error()))
	}

	if err := container.Server.Shutdown(context.Background()); err != nil {
		a.logger.Error("server shutdown error", slog.String("error", err.Error()))
	}

	return nil
}
