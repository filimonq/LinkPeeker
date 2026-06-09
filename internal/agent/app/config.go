package app

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

const (
	defaultInputTopic                 = "link.raw-updates"
	defaultDLQTopic                   = "link.raw-updates-dlq"
	defaultOutputTopic                = "link.processed-updates"
	defaultGroupID                    = "ai-agent-consumer-group"
	defaultConfigPath                 = "configs/ai-agent.yaml"
	defaultAISummarizerTimeoutSeconds = 15
	summarizerProviderStub            = "stub"
	summarizerProviderOpenAI          = "openai"
	summarizerProviderHuggingFace     = "huggingface"
	defaultOpenAIBaseURL              = "https://api.openai.com/v1"
	defaultOpenAIModel                = "gpt-4.1-mini"
	defaultHuggingFaceBaseURL         = "https://router.huggingface.co/v1"
	defaultHuggingFaceModel           = "google/gemma-2-2b-it"
	defaultSummarizationPrompt        = "Summarize the following update in 2-3 sentences."

	defaultResilienceTimeout = 10_000
	defaultRetryAttempts     = 3
	defaultRetryDelay        = 500
	defaultCBWindowSize      = 10
	defaultCBMinCalls        = 5
	defaultCBFailureRate     = 50.0
	defaultCBHalfOpen        = 5
)

// KafkaConfig holds Kafka connection configuration.
type KafkaConfig struct {
	Brokers     []string
	InputTopic  string
	DLQTopic    string
	OutputTopic string
	GroupID     string
}

type FilteringConfig struct {
	StopWords       []string `yaml:"stop-words"`
	ExcludedAuthors []string `yaml:"excluded-authors"`
	MinLength       int      `yaml:"min-length"`
}

type SummarizationConfig struct {
	Threshold      int    `yaml:"threshold"`
	Provider       string `yaml:"provider"`
	BaseURL        string `yaml:"-"`
	APIKey         string `yaml:"-"`
	Model          string `yaml:"-"`
	Prompt         string `yaml:"-"`
	TimeoutSeconds int    `yaml:"-"`
}

type PrioritizationConfig struct {
	HighKeywords []string `yaml:"high-keywords"`
	LowKeywords  []string `yaml:"low-keywords"`
}

type GroupingConfig struct {
	WindowMS int `yaml:"window-ms"`
}

type ResilienceConfig struct {
	Timeout               time.Duration
	RetryAttempts         uint
	RetryDelay            time.Duration
	RetryableStatuses     []int
	CBSlidingWindowSize   uint32
	CBMinCalls            uint32
	CBFailureRate         float64
	CBHalfOpenCalls       uint32
	CBWaitDuration        time.Duration
	UseExponentialBackoff bool
}

// Config holds AI agent application configuration.
type Config struct {
	Kafka          KafkaConfig
	Filtering      FilteringConfig
	Summarization  SummarizationConfig
	Prioritization PrioritizationConfig
	Grouping       GroupingConfig
	Resilience     ResilienceConfig
}

type yamlConfig struct {
	AIAgent struct {
		Filtering      FilteringConfig      `yaml:"filtering"`
		Summarization  SummarizationConfig  `yaml:"summarization"`
		Prioritization PrioritizationConfig `yaml:"prioritization"`
		Grouping       GroupingConfig       `yaml:"grouping"`
	} `yaml:"ai-agent"`
}

// Load reads configuration from a YAML file and environment variables.
func Load() (*Config, error) {
	_ = godotenv.Load()

	fileConfig, err := loadYAMLConfig(getEnv("AI_AGENT_CONFIG_PATH", defaultConfigPath))
	if err != nil {
		return nil, err
	}

	return &Config{
		Kafka: loadKafkaConfig(),
		Filtering: FilteringConfig{
			StopWords:       fileConfig.AIAgent.Filtering.StopWords,
			ExcludedAuthors: fileConfig.AIAgent.Filtering.ExcludedAuthors,
			MinLength:       fileConfig.AIAgent.Filtering.MinLength,
		},
		Summarization: SummarizationConfig{
			Threshold:      fileConfig.AIAgent.Summarization.Threshold,
			Provider:       loadSummarizerProvider(fileConfig.AIAgent.Summarization.Provider),
			BaseURL:        loadSummarizerBaseURL(fileConfig.AIAgent.Summarization.Provider),
			APIKey:         os.Getenv("AI_AGENT_SUMMARIZER_API_KEY"),
			Model:          loadSummarizerModel(fileConfig.AIAgent.Summarization.Provider),
			Prompt:         getEnv("AI_AGENT_SUMMARIZER_PROMPT", defaultSummarizationPrompt),
			TimeoutSeconds: getEnvInt("AI_AGENT_SUMMARIZER_TIMEOUT_SECONDS", defaultAISummarizerTimeoutSeconds),
		},
		Prioritization: PrioritizationConfig{
			HighKeywords: fileConfig.AIAgent.Prioritization.HighKeywords,
			LowKeywords:  fileConfig.AIAgent.Prioritization.LowKeywords,
		},
		Grouping: GroupingConfig{
			WindowMS: fileConfig.AIAgent.Grouping.WindowMS,
		},
		Resilience: loadResilienceConfig(),
	}, nil
}

func loadSummarizerProvider(fileProvider string) string {
	return strings.ToLower(strings.TrimSpace(getEnv("AI_AGENT_SUMMARIZER_PROVIDER", fileProvider)))
}

func loadSummarizerBaseURL(fileProvider string) string {
	if baseURL := os.Getenv("AI_AGENT_SUMMARIZER_BASE_URL"); baseURL != "" {
		return baseURL
	}

	switch loadSummarizerProvider(fileProvider) {
	case summarizerProviderHuggingFace:
		return defaultHuggingFaceBaseURL
	default:
		return defaultOpenAIBaseURL
	}
}

func loadSummarizerModel(fileProvider string) string {
	if model := os.Getenv("AI_AGENT_SUMMARIZER_MODEL"); model != "" {
		return model
	}

	switch loadSummarizerProvider(fileProvider) {
	case summarizerProviderHuggingFace:
		return defaultHuggingFaceModel
	default:
		return defaultOpenAIModel
	}
}

func loadResilienceConfig() ResilienceConfig {
	return ResilienceConfig{
		Timeout:               time.Duration(getEnvInt("RESILIENCE_TIMEOUT_MS", defaultResilienceTimeout)) * time.Millisecond,
		RetryAttempts:         uint(getEnvInt("RESILIENCE_RETRY_ATTEMPTS", defaultRetryAttempts)),
		RetryDelay:            time.Duration(getEnvInt("RESILIENCE_RETRY_DELAY_MS", defaultRetryDelay)) * time.Millisecond,
		RetryableStatuses:     loadRetryableStatuses(),
		CBSlidingWindowSize:   uint32(getEnvInt("CB_SLIDING_WINDOW_SIZE", defaultCBWindowSize)),
		CBMinCalls:            uint32(getEnvInt("CB_MIN_CALLS", defaultCBMinCalls)),
		CBFailureRate:         getEnvFloat("CB_FAILURE_RATE", defaultCBFailureRate),
		CBHalfOpenCalls:       uint32(getEnvInt("CB_HALF_OPEN_CALLS", defaultCBHalfOpen)),
		CBWaitDuration:        time.Duration(getEnvInt("CB_WAIT_DURATION_SECONDS", 1)) * time.Second,
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

func loadYAMLConfig(path string) (*yamlConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("AI agent config file is not set or unreadable")
	}

	var cfg yamlConfig
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		return nil, errors.New("invalid AI agent config file")
	}

	return &cfg, nil
}

func loadKafkaConfig() KafkaConfig {
	brokers := []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}
	if s := os.Getenv("KAFKA_BROKERS"); s != "" {
		brokers = strings.Split(s, ",")
	}

	return KafkaConfig{
		Brokers:     brokers,
		InputTopic:  getEnv("AGENT_KAFKA_INPUT_TOPIC", defaultInputTopic),
		DLQTopic:    getEnv("AGENT_KAFKA_DLQ_TOPIC", defaultDLQTopic),
		OutputTopic: getEnv("AGENT_KAFKA_OUTPUT_TOPIC", defaultOutputTopic),
		GroupID:     getEnv("AGENT_KAFKA_GROUP_ID", defaultGroupID),
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed
		}
	}
	return defaultVal
}
