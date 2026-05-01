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
	NotifierHTTP        NotifierType = "http"
	NotifierKafka       NotifierType = "kafka"
	NotifierKafkaOutbox NotifierType = "kafka_outbox"

	defaultWorkerCount      = 4
	defaultBatchSize        = 10
	defaultScheduleInterval = time.Minute

	defaultOutboxPollInterval = 5 * time.Second
)

// KafkaConfig holds Kafka connection configuration.
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

// Config holds scrapper application configuration.
type Config struct {
	BotURL             string
	GitHubToken        string
	Port               string
	GRPCPort           string
	ScheduleInterval   time.Duration
	DatabaseURL        string
	AccessType         string
	WorkerCount        int
	BatchSize          int
	NotifierType       NotifierType
	Kafka              KafkaConfig
	OutboxPollInterval time.Duration
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	_ = godotenv.Load()

	botURL := os.Getenv("BOT_URL")
	if botURL == "" {
		return nil, errors.New("BOT_URL is not set")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("DATABASE_URL is not set")
	}

	return &Config{
		BotURL:             botURL,
		GitHubToken:        os.Getenv("GITHUB_TOKEN"),
		Port:               envOrDefault("SCRAPPER_PORT", "8081"),
		GRPCPort:           envOrDefault("SCRAPPER_GRPC_PORT", "50051"),
		ScheduleInterval:   loadDuration("SCHEDULE_INTERVAL_SECONDS", defaultScheduleInterval),
		DatabaseURL:        dbURL,
		AccessType:         envOrDefault("ACCESS_TYPE", "sql"),
		WorkerCount:        loadInt("WORKER_COUNT", defaultWorkerCount),
		BatchSize:          loadInt("BATCH_SIZE", defaultBatchSize),
		NotifierType:       loadNotifierType(),
		Kafka:              loadKafkaConfig(),
		OutboxPollInterval: loadDuration("OUTBOX_POLL_INTERVAL_SECONDS", defaultOutboxPollInterval),
	}, nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadInt(key string, def int) int {
	if s := os.Getenv(key); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

func loadDuration(key string, def time.Duration) time.Duration {
	if s := os.Getenv(key); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return time.Duration(v) * time.Second
		}
	}
	return def
}

func loadNotifierType() NotifierType {
	if nt := os.Getenv("NOTIFIER_TYPE"); nt != "" {
		switch NotifierType(nt) {
		case NotifierHTTP, NotifierKafka, NotifierKafkaOutbox:
			return NotifierType(nt)
		}
	}
	return NotifierKafkaOutbox
}

func loadKafkaConfig() KafkaConfig {
	brokers := []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}
	if s := os.Getenv("KAFKA_BROKERS"); s != "" {
		brokers = strings.Split(s, ",")
	}
	return KafkaConfig{
		Brokers: brokers,
		Topic:   envOrDefault("KAFKA_TOPIC", "link-updates"),
	}
}
