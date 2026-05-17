// Package main is the entry point for the agent service.
package main

import (
	"log/slog"
	"os"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/app"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := app.Load()
	if err != nil {
		logger.Error("config load failed", "err", err)
		os.Exit(1)
	}

	application := app.New(logger, cfg)
	if err = application.Run(); err != nil {
		logger.Error("app terminated with error", "err", err)
		os.Exit(1)
	}
}
