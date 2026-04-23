package app

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type NotifierType string

const (
	NotifierHTTP  NotifierType = "http"
	NotifierKafka NotifierType = "kafka"
)

// KafkaConfig holds Kafka connection configuration.
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

// Config holds scrapper application configuration.
type Config struct {
	BotURL           string
	GitHubToken      string
	Port             string
	GRPCPort         string
	ScheduleInterval time.Duration
	DatabaseURL      string
	AccessType       string // "sql" or "squirrel"
	WorkerCount      int
	BatchSize        int
	NotifierType     NotifierType
	Kafka            KafkaConfig
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

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("DATABASE_URL is not set")
	}

	accessType := os.Getenv("ACCESS_TYPE")
	if accessType == "" {
		accessType = "sql"
	}

	interval := time.Minute
	if intervalStr := os.Getenv("SCHEDULE_INTERVAL_SECONDS"); intervalStr != "" {
		if seconds, err := strconv.Atoi(intervalStr); err == nil {
			interval = time.Duration(seconds) * time.Second
		}
	}

	workerCount := 4
	if wcStr := os.Getenv("WORKER_COUNT"); wcStr != "" {
		if val, err := strconv.Atoi(wcStr); err == nil {
			workerCount = val
		}
	}

	batchSize := 10
	if bsStr := os.Getenv("BATCH_SIZE"); bsStr != "" {
		if val, err := strconv.Atoi(bsStr); err == nil {
			batchSize = val
		}
	}

	notifierType := NotifierKafka
	if nt := os.Getenv("NOTIFIER_TYPE"); nt == string(NotifierHTTP) {
		notifierType = NotifierHTTP
	}

	kafkaBrokers := []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}
	if brokersStr := os.Getenv("KAFKA_BROKERS"); brokersStr != "" {
		kafkaBrokers = strings.Split(brokersStr, ",")
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "link-updates"
	}

	return &Config{
		BotURL:           botURL,
		GitHubToken:      os.Getenv("GITHUB_TOKEN"),
		Port:             port,
		GRPCPort:         grpcPort,
		ScheduleInterval: interval,
		DatabaseURL:      dbURL,
		AccessType:       accessType,
		WorkerCount:      workerCount,
		BatchSize:        batchSize,
		NotifierType:     notifierType,
		Kafka: KafkaConfig{
			Brokers: kafkaBrokers,
			Topic:   kafkaTopic,
		},
	}, nil
}
