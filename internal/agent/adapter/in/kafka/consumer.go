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
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/observability"
)

const (
	maxBytes       = 10 * 1024 * 1024 // 10 MB
	fetchBaseDelay = 1 * time.Second
	fetchMaxDelay  = 30 * time.Second
	fetchBackoff   = 2
	dlqRetryDelay  = 200 * time.Millisecond
	dlqMaxAttempts = 3
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
	metrics   *observability.AgentMetrics
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

func WithMetrics(c *Consumer, metrics *observability.AgentMetrics) {
	if c != nil {
		c.metrics = metrics
	}
}

// Start begins consuming messages until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("ai agent consumer started")
	fetchDelay := fetchBaseDelay

	for {
		msg, nextDelay, shouldStop := c.fetchNextMessage(ctx, fetchDelay)
		if shouldStop {
			return
		}
		if nextDelay != fetchBaseDelay {
			fetchDelay = nextDelay
			continue
		}
		fetchDelay = nextDelay

		handleStartedAt := time.Now()
		shouldCommit, handleErr := c.handle(ctx, msg)
		if c.metrics != nil {
			c.metrics.ObserveOperationDuration("kafka", "consumer_handle", time.Since(handleStartedAt))
		}
		if handleErr != nil {
			if c.metrics != nil {
				c.metrics.ObservePipelineEvent("handle", "error")
			}
			c.logger.Error("failed to handle message",
				slog.String("err", handleErr.Error()),
				slog.Int64("offset", msg.Offset),
			)
		}

		if !shouldCommit {
			continue
		}

		if err := c.commitMessage(ctx, msg); err != nil {
			continue
		}

		c.logger.Info("processed update successfully",
			slog.Int64("offset", msg.Offset),
		)
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

func (c *Consumer) fetchNextMessage(ctx context.Context, fetchDelay time.Duration) (kafka.Message, time.Duration, bool) {
	fetchStartedAt := time.Now()
	msg, err := c.reader.FetchMessage(ctx)
	if c.metrics != nil {
		c.metrics.ObserveOperationDuration("kafka", "consumer_fetch", time.Since(fetchStartedAt))
	}
	if err == nil {
		if c.metrics != nil {
			c.metrics.ObservePipelineEvent("fetch", "success")
		}
		return msg, fetchBaseDelay, false
	}
	if ctx.Err() != nil {
		c.logger.Info("ai agent consumer stopped")
		return kafka.Message{}, fetchDelay, true
	}
	if c.metrics != nil {
		c.metrics.ObservePipelineEvent("fetch", "error")
	}
	c.logger.Error("failed to fetch message",
		slog.String("err", err.Error()),
		slog.Duration("retry_in", fetchDelay),
	)

	nextDelay, shouldStop := c.waitFetchRetry(ctx, fetchDelay)
	return kafka.Message{}, nextDelay, shouldStop
}

func (c *Consumer) waitFetchRetry(ctx context.Context, fetchDelay time.Duration) (time.Duration, bool) {
	select {
	case <-ctx.Done():
		c.logger.Info("ai agent consumer stopped")
		return fetchDelay, true
	case <-time.After(fetchDelay):
	}

	nextDelay := fetchDelay * fetchBackoff
	if nextDelay > fetchMaxDelay {
		nextDelay = fetchMaxDelay
	}
	return nextDelay, false
}

func (c *Consumer) commitMessage(ctx context.Context, msg kafka.Message) error {
	commitStartedAt := time.Now()
	err := c.reader.CommitMessages(ctx, msg)
	if c.metrics != nil {
		c.metrics.ObserveOperationDuration("kafka", "consumer_commit", time.Since(commitStartedAt))
	}
	if err != nil {
		if c.metrics != nil {
			c.metrics.ObservePipelineEvent("commit", "error")
		}
		c.logger.Error("failed to commit message", slog.String("err", err.Error()))
		return fmt.Errorf("commit kafka message: %w", err)
	}
	if c.metrics != nil {
		c.metrics.ObservePipelineEvent("commit", "success")
	}
	return nil
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) (bool, error) {
	var update pkgdomain.LinkUpdate
	if err := json.Unmarshal(msg.Value, &update); err != nil {
		if c.metrics != nil {
			c.metrics.ObservePipelineEvent("deserialize", "error")
		}
		c.logger.Error("failed to deserialize raw update",
			slog.String("err", err.Error()),
			slog.Int64("offset", msg.Offset),
		)
		if dlqErr := c.sendToDLQ(ctx, msg, err); dlqErr != nil {
			return false, fmt.Errorf("send invalid message to dlq: %w", dlqErr)
		}
		return true, nil
	}
	if c.metrics != nil {
		c.metrics.ObservePipelineEvent("deserialize", "success")
	}

	if err := c.processor.Process(ctx, update); err != nil {
		return false, fmt.Errorf("process update: %w", err)
	}

	return true, nil
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, reason error) error {
	if c.dlqWriter == nil {
		c.logger.Warn("dlq writer is not configured, skipping dlq publish",
			slog.String("reason", reason.Error()),
		)
		return nil
	}

	dlqMessage := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "error", Value: []byte(fmt.Errorf("%w: %w", errDeserialize, reason).Error())},
			{Key: "original-topic", Value: []byte(msg.Topic)},
		},
	}

	var err error
	for attempt := 1; attempt <= dlqMaxAttempts; attempt++ {
		err = c.dlqWriter.WriteMessages(ctx, dlqMessage)
		if err == nil {
			return nil
		}

		c.logger.Warn("failed to send message to DLQ, retrying",
			slog.String("err", err.Error()),
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", dlqMaxAttempts),
		)

		if attempt == dlqMaxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("dlq publish canceled: %w", ctx.Err())
		case <-time.After(dlqRetryDelay):
		}
	}

	return fmt.Errorf("write message to dlq: %w", err)
}
