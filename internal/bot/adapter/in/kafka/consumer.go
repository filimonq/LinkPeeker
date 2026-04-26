// Package kafka provides Kafka consumer for bot service.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

const (
	maxBytes       = 10 * 1024 * 1024 // 10 MB
	retryBaseDelay = 1 * time.Second
)

// ErrDeserialize signals that message cannot be parsed.
var ErrDeserialize = errors.New("deserialization error")

// ErrValidation signals that message fields are invalid.
var ErrValidation = errors.New("validation error")

// Consumer reads link update notifications from Kafka.
type Consumer struct {
	reader     *kafka.Reader
	dlqWriter  *kafka.Writer
	svc        service.UpdateService
	logger     *slog.Logger
	maxRetries int
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(
	brokers []string,
	topic, groupID, dlqTopic string,
	maxRetries int,
	svc service.UpdateService,
	logger *slog.Logger,
) *Consumer {
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
		reader:     reader,
		dlqWriter:  dlqWriter,
		svc:        svc,
		logger:     logger,
		maxRetries: maxRetries,
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

		c.handle(ctx, msg)

		if err = c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit message", slog.String("err", err.Error()))
		}
	}
}

// Close closes the underlying Kafka reader and DLQ writer.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("close kafka reader: %w", err)
	}
	if err := c.dlqWriter.Close(); err != nil {
		return fmt.Errorf("close dlq writer: %w", err)
	}
	return nil
}

// handle processes a message with retry logic and DLQ fallback.
func (c *Consumer) handle(ctx context.Context, msg kafka.Message) {
	err := c.process(ctx, msg)
	if err == nil {
		return
	}

	if errors.Is(err, ErrDeserialize) || errors.Is(err, ErrValidation) {
		c.logger.Warn("unrecoverable error, forwarding to DLQ",
			slog.String("reason", err.Error()),
			slog.Int64("offset", msg.Offset),
		)
		c.sendToDLQ(ctx, msg, err)
		return
	}

	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		delay := retryBaseDelay * time.Duration(attempt)
		c.logger.Warn("retrying message",
			slog.Int("attempt", attempt),
			slog.Int("max", c.maxRetries),
			slog.Duration("delay", delay),
			slog.Int64("offset", msg.Offset),
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		if err = c.process(ctx, msg); err == nil {
			return
		}
	}

	c.logger.Error("all retries exhausted, sending to DLQ",
		slog.String("err", err.Error()),
		slog.Int64("offset", msg.Offset),
	)
	c.sendToDLQ(ctx, msg, err)
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	var update pkgdomain.LinkUpdate
	if err := json.Unmarshal(msg.Value, &update); err != nil {
		return fmt.Errorf("%w: %w", ErrDeserialize, err)
	}

	if err := validate(update); err != nil {
		return fmt.Errorf("%w: %w", ErrValidation, err)
	}

	if err := c.svc.SendUpdate(ctx, update); err != nil {
		return fmt.Errorf("send update: %w", err)
	}

	return nil
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, reason error) {
	err := c.dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "error", Value: []byte(reason.Error())},
			{Key: "original-topic", Value: []byte(msg.Topic)},
		},
	})
	if err != nil {
		c.logger.Error("failed to send message to DLQ", slog.String("err", err.Error()))
	}
}

func validate(update pkgdomain.LinkUpdate) error {
	if update.URL == "" {
		return errors.New("url is empty")
	}
	if len(update.TgChatIDs) == 0 {
		return errors.New("tg_chat_ids is empty")
	}
	return nil
}
