// Package app assembles and runs the application.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
)

// App is the main application container.
type App struct {
	logger *slog.Logger
	cfg    *Config
}

// New creates a new App instance.
func New(logger *slog.Logger, cfg *Config) *App {
	return &App{logger: logger, cfg: cfg}
}

// Run initializes and starts the bot application.
func (a *App) Run() error {
	container, err := NewContainer(a.cfg, a.logger)
	if err != nil {
		return fmt.Errorf("build container: %w", err)
	}

	if err := container.Registry.Register(context.Background(), container.Metas); err != nil {
		a.logger.Warn("failed to register commands", slog.String("error", err.Error()))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	a.logger.Info("bot is starting")
	container.Bot.Start(ctx)
	a.logger.Info("bot stopped")

	return nil
}
