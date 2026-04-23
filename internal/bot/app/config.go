package app

import (
	"errors"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// KafkaConfig holds Kafka connection configuration.
type KafkaConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// Config holds application configuration.
type Config struct {
	TelegramToken  string
	ScrapperURL    string
	Port           string
	TelegramAPIURL string // для test containers
	DatabaseURL    string
	AccessType     string // "pgx" or "squirrel"
	Kafka          KafkaConfig
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

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("DATABASE_URL is not set")
	}

	accessType := os.Getenv("ACCESS_TYPE")
	if accessType == "" {
		accessType = "pgx"
	}

	kafkaBrokers := []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}
	if brokersStr := os.Getenv("KAFKA_BROKERS"); brokersStr != "" {
		kafkaBrokers = strings.Split(brokersStr, ",")
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "link-updates"
	}

	kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")
	if kafkaGroupID == "" {
		kafkaGroupID = "bot-consumer-group"
	}

	return &Config{
		TelegramToken:  token,
		ScrapperURL:    scrapperURL,
		Port:           port,
		TelegramAPIURL: os.Getenv("TELEGRAM_API_URL"),
		DatabaseURL:    dbURL,
		AccessType:     accessType,
		Kafka: KafkaConfig{
			Brokers: kafkaBrokers,
			Topic:   kafkaTopic,
			GroupID: kafkaGroupID,
		},
	}, nil
}
