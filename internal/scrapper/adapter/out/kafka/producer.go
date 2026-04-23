// Package kafka provides Kafka-based notifier for bot service.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/segmentio/kafka-go"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

// Producer sends link update notifications via Kafka.
type Producer struct {
	writer *kafka.Writer
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

// SendUpdate sends a link update to Kafka topic.
func (p *Producer) SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error {
	body, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal update: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatInt(update.ID, 10)),
		Value: body,
	})
	if err != nil {
		return fmt.Errorf("write kafka message: %w", err)
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
