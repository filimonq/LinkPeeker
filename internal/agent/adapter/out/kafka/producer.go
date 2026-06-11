// Package kafka provides Kafka producer for AI agent output.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/observability"
)

// Producer sends processed link updates via Kafka.
type Producer struct {
	writer  *kafka.Writer
	metrics *observability.AgentMetrics
}

// NewProducer creates a new Kafka producer.
func NewProducer(brokers []string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: false,
		RequiredAcks:           kafka.RequireAll,
	}

	return &Producer{writer: writer}
}

func WithMetrics(p *Producer, metrics *observability.AgentMetrics) {
	if p != nil {
		p.metrics = metrics
	}
}

// SendUpdate sends a processed update to Kafka topic.
func (p *Producer) SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error {
	start := time.Now()
	body, err := json.Marshal(update)
	if err != nil {
		if p.metrics != nil {
			p.metrics.ObservePipelineEvent("publish", "error")
		}
		return fmt.Errorf("marshal update: %w", err)
	}

	if err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatInt(update.ID, 10)),
		Value: body,
	}); err != nil {
		if p.metrics != nil {
			p.metrics.ObserveOperationDuration("kafka", "producer_write", time.Since(start))
			p.metrics.ObservePipelineEvent("publish", "error")
		}
		return fmt.Errorf("write kafka message: %w", err)
	}
	if p.metrics != nil {
		p.metrics.ObserveOperationDuration("kafka", "producer_write", time.Since(start))
		p.metrics.ObservePipelineEvent("publish", "success")
	}

	return nil
}

// Close closes the underlying Kafka writer.
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close kafka writer: %w", err)
	}
	return nil
}
