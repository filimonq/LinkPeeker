package app

import (
	"log/slog"

	kafkaconsumer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/adapter/in/kafka"
	kafkaproducer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/adapter/out/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/service"
)

// Container holds AI agent dependencies.
type Container struct {
	KafkaConsumer *kafkaconsumer.Consumer
	KafkaProducer *kafkaproducer.Producer
}

// NewContainer builds and wires all dependencies.
func NewContainer(cfg *Config, logger *slog.Logger) (*Container, error) {
	producer := kafkaproducer.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.OutputTopic)
	summarizer := service.NewStubSummarizer()
	processor := service.NewProcessor(service.Config{
		Filtering: service.FilteringConfig{
			StopWords:       cfg.Filtering.StopWords,
			ExcludedAuthors: cfg.Filtering.ExcludedAuthors,
			MinLength:       cfg.Filtering.MinLength,
		},
		Summarization: service.SummarizationConfig{
			Threshold: cfg.Summarization.Threshold,
		},
	}, summarizer, producer)

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
	}, nil
}
