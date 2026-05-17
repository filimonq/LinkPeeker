// Package kafka provides Kafka consumer for AI agent input.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

const maxBytes = 10 * 1024 * 1024 // 10 MB

// Processor handles incoming updates.
type Processor interface {
	Process(ctx context.Context, update pkgdomain.LinkUpdate) error
}

// Consumer reads raw updates from Kafka and forwards them to the processor.
type Consumer struct {
	reader    *kafka.Reader
	processor Processor
	logger    *slog.Logger
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(brokers []string, topic, groupID string, processor Processor, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: maxBytes,
	})

	return &Consumer{
		reader:    reader,
		processor: processor,
		logger:    logger,
	}
}

// Start begins consuming messages until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("ai agent consumer started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("ai agent consumer stopped")
				return
			}
			c.logger.Error("failed to fetch message", slog.String("err", err.Error()))
			continue
		}

		shouldCommit, handleErr := c.handle(ctx, msg)
		if handleErr != nil {
			c.logger.Error("failed to handle message",
				slog.String("err", handleErr.Error()),
				slog.Int64("offset", msg.Offset),
			)
		}

		if !shouldCommit {
			continue
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

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) (bool, error) {
	var update pkgdomain.LinkUpdate
	if err := json.Unmarshal(msg.Value, &update); err != nil {
		c.logger.Error("failed to deserialize raw update",
			slog.String("err", err.Error()),
			slog.Int64("offset", msg.Offset),
		)
		return true, nil
	}

	if err := c.processor.Process(ctx, update); err != nil {
		return false, fmt.Errorf("process update: %w", err)
	}

	return true, nil
}
