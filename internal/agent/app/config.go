package app

import (
	"errors"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

const (
	defaultInputTopic  = "link.raw-updates"
	defaultOutputTopic = "link.processed-updates"
	defaultGroupID     = "ai-agent-consumer-group"
	defaultConfigPath  = "configs/ai-agent.yaml"
)

// KafkaConfig holds Kafka connection configuration.
type KafkaConfig struct {
	Brokers     []string
	InputTopic  string
	OutputTopic string
	GroupID     string
}

type FilteringConfig struct {
	StopWords       []string `yaml:"stop-words"`
	ExcludedAuthors []string `yaml:"excluded-authors"`
	MinLength       int      `yaml:"min-length"`
}

type SummarizationConfig struct {
	Threshold int `yaml:"threshold"`
}

// Config holds AI agent application configuration.
type Config struct {
	Kafka         KafkaConfig
	Filtering     FilteringConfig
	Summarization SummarizationConfig
}

type yamlConfig struct {
	AIAgent struct {
		Filtering     FilteringConfig     `yaml:"filtering"`
		Summarization SummarizationConfig `yaml:"summarization"`
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
			Threshold: fileConfig.AIAgent.Summarization.Threshold,
		},
	}, nil
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
