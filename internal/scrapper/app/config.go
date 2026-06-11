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

	defaultPort               = "8081"
	defaultGRPCPort           = "50051"
	defaultAccessType         = "sql"
	defaultKafkaTopic         = "link.raw-updates"
	defaultWorkerCount        = 4
	defaultBatchSize          = 10
	defaultOutboxPollInterval = 5 * time.Second
	defaultCacheTTL           = 60 * time.Second

	defaultResilienceTimeout = 10_000
	defaultRetryAttempts     = 3
	defaultRetryDelay        = 500
	defaultCBWindowSize      = 10
	defaultCBMinCalls        = 5
	defaultCBFailureRate     = 50.0
	defaultCBHalfOpen        = 5
	defaultRateLimitRPS      = 10.0
	defaultRateLimitBurst    = 20
)

var defaultKafkaBrokers = []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}
var defaultValkeyAddrs = []string{"valkey-1:6379", "valkey-2:6379", "valkey-3:6379"}

// KafkaConfig holds Kafka connection configuration.
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

// ValkeyConfig holds Valkey cluster configuration.
type ValkeyConfig struct {
	Addrs    []string
	Password string
	TTL      time.Duration
}

type ResilienceConfig struct {
	Timeout           time.Duration
	RetryAttempts     uint
	RetryDelay        time.Duration
	RetryableStatuses []int

	CBSlidingWindowSize uint32
	CBMinCalls          uint32
	CBFailureRate       float64
	CBHalfOpenCalls     uint32
	CBWaitDuration      time.Duration

	RateLimitRPS          float64
	RateLimitBurst        int
	UseExponentialBackoff bool
}

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
	Valkey             ValkeyConfig
	Resilience         ResilienceConfig
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
		Port:               envOrDefault("SCRAPPER_PORT", defaultPort),
		GRPCPort:           envOrDefault("SCRAPPER_GRPC_PORT", defaultGRPCPort),
		DatabaseURL:        dbURL,
		AccessType:         envOrDefault("ACCESS_TYPE", defaultAccessType),
		ScheduleInterval:   loadDuration("SCHEDULE_INTERVAL_SECONDS", time.Minute),
		WorkerCount:        loadInt("WORKER_COUNT", defaultWorkerCount),
		BatchSize:          loadInt("BATCH_SIZE", defaultBatchSize),
		NotifierType:       loadNotifierType(),
		Kafka:              loadKafkaConfig(),
		OutboxPollInterval: loadDuration("OUTBOX_POLL_INTERVAL_SECONDS", defaultOutboxPollInterval),
		Valkey:             loadValkeyConfig(),
		Resilience:         loadResilienceConfig(),
	}, nil
}

func loadResilienceConfig() ResilienceConfig {
	return ResilienceConfig{
		Timeout:               time.Duration(loadInt("RESILIENCE_TIMEOUT_MS", defaultResilienceTimeout)) * time.Millisecond,
		RetryAttempts:         uint(loadInt("RESILIENCE_RETRY_ATTEMPTS", defaultRetryAttempts)),
		RetryDelay:            time.Duration(loadInt("RESILIENCE_RETRY_DELAY_MS", defaultRetryDelay)) * time.Millisecond,
		RetryableStatuses:     loadRetryableStatuses(),
		CBSlidingWindowSize:   uint32(loadInt("CB_SLIDING_WINDOW_SIZE", defaultCBWindowSize)),
		CBMinCalls:            uint32(loadInt("CB_MIN_CALLS", defaultCBMinCalls)),
		CBFailureRate:         loadFloat("CB_FAILURE_RATE", defaultCBFailureRate),
		CBHalfOpenCalls:       uint32(loadInt("CB_HALF_OPEN_CALLS", defaultCBHalfOpen)),
		CBWaitDuration:        loadDuration("CB_WAIT_DURATION_SECONDS", 1*time.Second),
		RateLimitRPS:          loadFloat("RATE_LIMIT_RPS", defaultRateLimitRPS),
		RateLimitBurst:        loadInt("RATE_LIMIT_BURST", defaultRateLimitBurst),
		UseExponentialBackoff: os.Getenv("RETRY_EXPONENTIAL_BACKOFF") == "true",
	}
}

func loadRetryableStatuses() []int {
	s := os.Getenv("RETRY_STATUSES")
	if s == "" {
		return []int{500, 502, 503, 504}
	}
	parts := strings.Split(s, ",")
	statuses := make([]int, 0, len(parts))
	for _, p := range parts {
		if v, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
			statuses = append(statuses, v)
		}
	}
	return statuses
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

func loadFloat(key string, def float64) float64 {
	if s := os.Getenv(key); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
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
	brokers := defaultKafkaBrokers
	if s := os.Getenv("KAFKA_BROKERS"); s != "" {
		brokers = strings.Split(s, ",")
	}
	return KafkaConfig{
		Brokers: brokers,
		Topic:   envOrDefault("SCRAPPER_KAFKA_TOPIC", defaultKafkaTopic),
	}
}

func loadValkeyConfig() ValkeyConfig {
	addrs := defaultValkeyAddrs
	if s := os.Getenv("VALKEY_ADDRS"); s != "" {
		addrs = strings.Split(s, ",")
	}
	return ValkeyConfig{
		Addrs:    addrs,
		Password: os.Getenv("VALKEY_PASSWORD"),
		TTL:      loadDuration("CACHE_TTL_SECONDS", defaultCacheTTL),
	}
}
