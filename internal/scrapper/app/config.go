package app

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds scrapper application configuration.
type Config struct {
	BotURL           string
	GitHubToken      string
	Port             string
	GRPCPort         string
	ScheduleInterval time.Duration
	DatabaseURL      string
	AccessType       string // "sql" or "squirrel"
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	_ = godotenv.Load()

	botURL := os.Getenv("BOT_URL")
	if botURL == "" {
		return nil, errors.New("BOT_URL is not set")
	}

	port := os.Getenv("SCRAPPER_PORT")
	if port == "" {
		port = "8081"
	}

	grpcPort := os.Getenv("SCRAPPER_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	intervalStr := os.Getenv("SCHEDULE_INTERVAL_SECONDS")
	interval := time.Minute

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("DATABASE_URL is not set")
	}

	accessType := os.Getenv("ACCESS_TYPE")
	if accessType == "" {
		accessType = "sql"
	}

	if intervalStr != "" {
		seconds, err := strconv.Atoi(intervalStr)
		if err == nil {
			interval = time.Duration(seconds) * time.Second
		}
	}

	return &Config{
		BotURL:           botURL,
		GitHubToken:      os.Getenv("GITHUB_TOKEN"),
		Port:             port,
		GRPCPort:         grpcPort,
		ScheduleInterval: interval,
		DatabaseURL:      dbURL,
		AccessType:       accessType,
	}, nil
}
