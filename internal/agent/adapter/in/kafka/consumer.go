// Package kafka provides Kafka consumer for AI agent input.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

const (
	maxBytes       = 10 * 1024 * 1024 // 10 MB
	fetchBaseDelay = 1 * time.Second
	fetchMaxDelay  = 30 * time.Second
)

var errDeserialize = errors.New("deserialization error")

// Processor handles incoming updates.
type Processor interface {
	Process(ctx context.Context, update pkgdomain.LinkUpdate) error
}

// Consumer reads raw updates from Kafka and forwards them to the processor.
type Consumer struct {
	reader    *kafka.Reader
	dlqWriter *kafka.Writer
	processor Processor
	logger    *slog.Logger
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(brokers []string, topic, groupID, dlqTopic string, processor Processor, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: maxBytes,
	})

	dlqWriter := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  dlqTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: false,
		RequiredAcks:           kafka.RequireAll,
	}

	return &Consumer{
		reader:    reader,
		dlqWriter: dlqWriter,
		processor: processor,
		logger:    logger,
	}
}

// Start begins consuming messages until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("ai agent consumer started")
	fetchDelay := fetchBaseDelay

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("ai agent consumer stopped")
				return
			}
			c.logger.Error("failed to fetch message",
				slog.String("err", err.Error()),
				slog.Duration("retry_in", fetchDelay),
			)
			select {
			case <-ctx.Done():
				c.logger.Info("ai agent consumer stopped")
				return
			case <-time.After(fetchDelay):
			}
			fetchDelay *= 2
			if fetchDelay > fetchMaxDelay {
				fetchDelay = fetchMaxDelay
			}
			continue
		}
		fetchDelay = fetchBaseDelay

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

		c.logger.Info("processed update successfully",
			slog.Int64("offset", msg.Offset),
		)

		if err = c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit message", slog.String("err", err.Error()))
			continue
		}
	}
}

// Close closes the underlying Kafka reader.
func (c *Consumer) Close() error {
	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			return fmt.Errorf("close kafka reader: %w", err)
		}
	}
	if c.dlqWriter != nil {
		if err := c.dlqWriter.Close(); err != nil {
			return fmt.Errorf("close dlq writer: %w", err)
		}
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
		c.sendToDLQ(ctx, msg, err)
		return true, nil
	}

	if err := c.processor.Process(ctx, update); err != nil {
		return false, fmt.Errorf("process update: %w", err)
	}

	return true, nil
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, reason error) {
	if c.dlqWriter == nil {
		c.logger.Warn("dlq writer is not configured, skipping dlq publish",
			slog.String("reason", reason.Error()),
		)
		return
	}

	if err := c.dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "error", Value: []byte(fmt.Errorf("%w: %w", errDeserialize, reason).Error())},
			{Key: "original-topic", Value: []byte(msg.Topic)},
		},
	}); err != nil {
		c.logger.Error("failed to send message to DLQ", slog.String("err", err.Error()))
	}
}
