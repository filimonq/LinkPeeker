// Package app assembles and runs the application.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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

	if err = container.Registry.Register(context.Background(), container.Metas); err != nil {
		a.logger.Warn("failed to register commands", slog.String("error", err.Error()))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		a.logger.Info("http server starting", slog.String("port", a.cfg.Port))
		if err = container.HTTPServer.Start(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				a.logger.Error("http server error", slog.String("error", err.Error()))
			}
		}
	}()

	go func() {
		container.KafkaConsumer.Start(ctx)
	}()

	a.logger.Info("bot is starting")
	container.Bot.Start(ctx)
	a.logger.Info("bot stopped")

	if err = container.KafkaConsumer.Close(); err != nil {
		a.logger.Error("kafka consumer close error", slog.String("error", err.Error()))
	}

	if err = container.HTTPServer.Shutdown(context.Background()); err != nil {
		a.logger.Error("http server shutdown error", slog.String("error", err.Error()))
	}

	return nil
}
