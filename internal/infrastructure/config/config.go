package config

import (
	"errors"
	"os"
)

type Config struct {
	TelegramToken string
}

func Load() (*Config, error) {
	token := os.Getenv("TELEGRAM_TOKEN")
	if token == "" {
		return nil, errors.New("TELEGRAM_TOKEN environment variable is not set")
	}

	return &Config{TelegramToken: token}, nil
}
