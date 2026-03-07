// Package main is the entry point for the scrapper service.
package main

import (
	"log/slog"
	"os"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/app"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := app.Load()
	if err != nil {
		logger.Error("config load failed", "err", err)
		os.Exit(1)
	}

	if err := app.New(logger, cfg).Run(); err != nil {
		logger.Error("scrapper terminated with error", "err", err)
		os.Exit(1)
	}
}
