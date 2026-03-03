package app

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

// Config holds application configuration.
type Config struct {
	TelegramToken string
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	_ = godotenv.Load()

	token := os.Getenv("TELEGRAM_TOKEN")
	if token == "" {
		return nil, errors.New("TELEGRAM_TOKEN is not set")
	}

	return &Config{TelegramToken: token}, nil
}
