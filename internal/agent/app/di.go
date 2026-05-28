package app

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	kafkaconsumer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/adapter/in/kafka"
	kafkaproducer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/adapter/out/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/service"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/resilience"
)

// Container holds AI agent dependencies.
type Container struct {
	KafkaConsumer *kafkaconsumer.Consumer
	KafkaProducer *kafkaproducer.Producer
	Grouper       *service.Grouper
}

// NewContainer builds and wires all dependencies.
func NewContainer(cfg *Config, logger *slog.Logger) (*Container, error) {
	producer := kafkaproducer.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.OutputTopic)
	summarizer, err := buildSummarizer(cfg)
	if err != nil {
		return nil, err
	}
	grouper := service.NewGrouper(time.Duration(cfg.Grouping.WindowMS)*time.Millisecond, producer, logger)
	processor := service.NewProcessor(service.Config{
		Filtering: service.FilteringConfig{
			StopWords:       cfg.Filtering.StopWords,
			ExcludedAuthors: cfg.Filtering.ExcludedAuthors,
			MinLength:       cfg.Filtering.MinLength,
		},
		Summarization: service.SummarizationConfig{
			Threshold: cfg.Summarization.Threshold,
		},
		Prioritization: service.PrioritizationConfig{
			HighKeywords: cfg.Prioritization.HighKeywords,
			LowKeywords:  cfg.Prioritization.LowKeywords,
		},
	}, summarizer, grouper)

	consumer := kafkaconsumer.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.InputTopic,
		cfg.Kafka.GroupID,
		processor,
		logger,
	)

	return &Container{
		KafkaConsumer: consumer,
		KafkaProducer: producer,
		Grouper:       grouper,
	}, nil
}

func buildSummarizer(cfg *Config) (service.Summarizer, error) {
	provider := strings.ToLower(strings.TrimSpace(cfg.Summarization.Provider))
	if provider == "" || provider == summarizerProviderStub {
		return service.NewStubSummarizer(), nil
	}

	if provider != summarizerProviderOpenAI && provider != summarizerProviderHuggingFace {
		return nil, fmt.Errorf("unsupported summarizer provider: %s", cfg.Summarization.Provider)
	}
	if strings.TrimSpace(cfg.Summarization.APIKey) == "" {
		return nil, errors.New("AI_AGENT_SUMMARIZER_API_KEY is required for remote summarizer")
	}

	return service.NewOpenAISummarizer(
		cfg.Summarization.BaseURL,
		cfg.Summarization.APIKey,
		cfg.Summarization.Model,
		cfg.Summarization.Prompt,
		time.Duration(cfg.Summarization.TimeoutSeconds)*time.Second,
		newResilientClient(cfg.Resilience, "ai-agent-"+provider+"-summarizer"),
	), nil
}

func newResilientClient(cfg ResilienceConfig, name string) *resilience.Client {
	return resilience.NewClient(resilience.Config{
		Timeout:               cfg.Timeout,
		RetryAttempts:         cfg.RetryAttempts,
		RetryDelay:            cfg.RetryDelay,
		RetryableStatuses:     cfg.RetryableStatuses,
		CBName:                name,
		CBSlidingWindowSize:   cfg.CBSlidingWindowSize,
		CBMinCalls:            cfg.CBMinCalls,
		CBFailureRate:         cfg.CBFailureRate,
		CBHalfOpenCalls:       cfg.CBHalfOpenCalls,
		CBWaitDuration:        cfg.CBWaitDuration,
		UseExponentialBackoff: cfg.UseExponentialBackoff,
	})
}
