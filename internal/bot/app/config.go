package app

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// KafkaConfig holds Kafka connection configuration.
type KafkaConfig struct {
	Brokers    []string
	Topic      string
	GroupID    string
	DLQTopic   string
	MaxRetries int
}

type RateLimitConfig struct {
	RPS   float64
	Burst int
}

type Config struct {
	TelegramToken  string
	ScrapperURL    string
	Port           string
	MetricsPort    string
	TelegramAPIURL string // для test containers
	DatabaseURL    string
	AccessType     string // "pgx" or "squirrel"
	Kafka          KafkaConfig
	RateLimit      RateLimitConfig
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

	return &Config{
		TelegramToken:  token,
		ScrapperURL:    scrapperURL,
		Port:           getEnv("BOT_PORT", "8080"),
		MetricsPort:    getEnv("BOT_METRICS_PORT", "8011"),
		TelegramAPIURL: os.Getenv("TELEGRAM_API_URL"),
		DatabaseURL:    getRequiredEnv("DATABASE_URL"),
		AccessType:     getEnv("ACCESS_TYPE", "pgx"),
		Kafka:          loadKafkaConfig(),
		RateLimit:      loadRateLimitConfig(),
	}, nil
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getRequiredEnv(key string) string {
	return os.Getenv(key)
}

func loadKafkaConfig() KafkaConfig {
	brokers := []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}
	if s := os.Getenv("KAFKA_BROKERS"); s != "" {
		brokers = strings.Split(s, ",")
	}

	maxRetries := 3
	if v, err := strconv.Atoi(os.Getenv("KAFKA_MAX_RETRIES")); err == nil {
		maxRetries = v
	}

	return KafkaConfig{
		Brokers:    brokers,
		Topic:      getEnv("BOT_KAFKA_TOPIC", "link.processed-updates"),
		GroupID:    getEnv("BOT_KAFKA_GROUP_ID", "bot-consumer-group"),
		DLQTopic:   getEnv("BOT_KAFKA_DLQ_TOPIC", "link.processed-updates-dlq"),
		MaxRetries: maxRetries,
	}
}

func loadRateLimitConfig() RateLimitConfig {
	rps := 10.0
	if v, err := strconv.ParseFloat(os.Getenv("RATE_LIMIT_RPS"), 64); err == nil {
		rps = v
	}
	burst := 20
	if v, err := strconv.Atoi(os.Getenv("RATE_LIMIT_BURST")); err == nil {
		burst = v
	}
	return RateLimitConfig{RPS: rps, Burst: burst}
}
