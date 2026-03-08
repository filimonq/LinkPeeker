package app

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

// Config holds application configuration.
type Config struct {
	TelegramToken  string
	ScrapperURL    string
	Port           string
	TelegramAPIURL string // для test containers
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	_ = godotenv.Load()

	token := os.Getenv("TELEGRAM_TOKEN")
	if token == "" {
		return nil, errors.New("TELEGRAM_TOKEN is not set")
	}

	scrapperURL := os.Getenv("SCRAPPER_URL")
	if scrapperURL == "" {
		return nil, errors.New("SCRAPPER_URL is not set")
	}

	port := os.Getenv("BOT_PORT")
	if port == "" {
		port = "8080"
	}

	return &Config{
		TelegramToken:  token,
		ScrapperURL:    scrapperURL,
		Port:           port,
		TelegramAPIURL: os.Getenv("TELEGRAM_API_URL"),
	}, nil
}
