// Package app assembles and runs the AI agent application.
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

// Run initializes and starts the AI agent application.
func (a *App) Run() error {
	container, err := NewContainer(a.cfg, a.logger)
	if err != nil {
		return fmt.Errorf("build container: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		a.logger.Info("agent metrics server starting", slog.String("port", a.cfg.MetricsPort))
		if err = container.MetricsServer.Start(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				a.logger.Error("metrics server error", slog.String("error", err.Error()))
			}
		}
	}()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		container.KafkaConsumer.Start(ctx)
	}()

	<-ctx.Done()
	a.logger.Info("ai agent is shutting down")

	if err = container.KafkaConsumer.Close(); err != nil {
		a.logger.Error("kafka consumer close error", slog.String("error", err.Error()))
	}
	<-consumerDone
	if err = container.Grouper.Close(context.Background()); err != nil {
		a.logger.Error("grouper close error", slog.String("error", err.Error()))
	}
	if err = container.KafkaProducer.Close(); err != nil {
		a.logger.Error("kafka producer close error", slog.String("error", err.Error()))
	}
	if err = container.MetricsServer.Shutdown(context.Background()); err != nil {
		a.logger.Error("metrics server shutdown error", slog.String("error", err.Error()))
	}

	return nil
}
