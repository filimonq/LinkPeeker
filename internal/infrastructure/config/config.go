// Package config provides application configuration loading.
package config

import (
	"errors"
	"os"
)

// Config holds application configuration.
type Config struct {
	TelegramToken string
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	token := os.Getenv("TELEGRAM_TOKEN")
	if token == "" {
		return nil, errors.New("TELEGRAM_TOKEN environment variable is not set")
	}

	return &Config{TelegramToken: token}, nil
}
