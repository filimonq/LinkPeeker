// Package kafka provides Kafka consumer for bot service.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

// Consumer reads link update notifications from Kafka.
type Consumer struct {
	reader *kafka.Reader
	svc    service.UpdateService
	logger *slog.Logger
}

const maxBytes = 10 * 1024 * 1024 // 10 MB

// NewConsumer creates a new Kafka consumer.
func NewConsumer(brokers []string, topic, groupID string, svc service.UpdateService, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: maxBytes,
	})

	return &Consumer{
		reader: reader,
		svc:    svc,
		logger: logger,
	}
}

// Start begins consuming messages until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("kafka consumer started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("kafka consumer stopped")
				return
			}
			c.logger.Error("failed to fetch message", slog.String("err", err.Error()))
			continue
		}

		if err = c.process(ctx, msg); err != nil {
			c.logger.Error("failed to process message",
				slog.String("err", err.Error()),
				slog.Int64("offset", msg.Offset),
			)
			// DLQ
		}

		if err = c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit message", slog.String("err", err.Error()))
		}
	}
}

// Close closes the underlying Kafka reader.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("close kafka reader: %w", err)
	}
	return nil
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	var update pkgdomain.LinkUpdate
	if err := json.Unmarshal(msg.Value, &update); err != nil {
		return fmt.Errorf("unmarshal message: %w", err)
	}

	if err := c.svc.SendUpdate(ctx, update); err != nil {
		return fmt.Errorf("send update: %w", err)
	}

	return nil
}
