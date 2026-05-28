//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkaconsumer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/adapter/in/kafka"
	kafkaproducer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/adapter/out/kafka"
	agentservice "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/service"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/integration"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.DialLeader(context.Background(), "tcp", brokers[0], topic, 0)
	require.NoError(t, err)
	_ = conn.Close()
}

func writeMsg(t *testing.T, brokers []string, topic string, value []byte) {
	t.Helper()
	w := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafkago.RequireOne,
	}
	t.Cleanup(func() { _ = w.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, w.WriteMessages(ctx, kafkago.Message{Value: value}))
}

func readProcessed(t *testing.T, brokers []string, topic string, timeout time.Duration) (pkgdomain.LinkUpdate, bool) {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		MinBytes:    1,
		MaxBytes:    10 * 1024 * 1024,
		StartOffset: kafkago.FirstOffset,
	})
	t.Cleanup(func() { _ = r.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msg, err := r.ReadMessage(ctx)
	if err != nil {
		return pkgdomain.LinkUpdate{}, false
	}

	var update pkgdomain.LinkUpdate
	if err = json.Unmarshal(msg.Value, &update); err != nil {
		return pkgdomain.LinkUpdate{}, false
	}

	return update, true
}

func TestConsumer_ValidMessage_Processed(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	const rawTopic = "test-agent-raw"
	const processedTopic = "test-agent-processed"
	createTopic(t, brokers, rawTopic)
	createTopic(t, brokers, processedTopic)

	producer := kafkaproducer.NewProducer(brokers, processedTopic)
	t.Cleanup(func() { _ = producer.Close() })
	grouper := agentservice.NewGrouper(50*time.Millisecond, producer)
	t.Cleanup(func() { _ = grouper.Close(context.Background()) })

	processor := agentservice.NewProcessor(agentservice.Config{
		Filtering: agentservice.FilteringConfig{
			MinLength: 10,
		},
		Summarization: agentservice.SummarizationConfig{
			Threshold: 500,
		},
		Prioritization: agentservice.PrioritizationConfig{
			HighKeywords: []string{"critical", "urgent", "breaking", "security"},
			LowKeywords:  []string{"minor", "typo", "chore", "docs"},
		},
	}, agentservice.NewStubSummarizer(), grouper)

	consumer := kafkaconsumer.NewConsumer(brokers, rawTopic, "cg-agent-valid", processor, noopLogger())
	t.Cleanup(func() { _ = consumer.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.Start(ctx)

	body, _ := json.Marshal(pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://github.com/x/y",
		Author:      "user",
		Description: "This is a critical update text that is definitely valid",
		TgChatIDs:   []int64{1, 2},
	})
	writeMsg(t, brokers, rawTopic, body)

	update, ok := readProcessed(t, brokers, processedTopic, 15*time.Second)
	require.True(t, ok, "валидное сообщение должно попасть в processed topic")
	assert.Equal(t, int64(1), update.ID)
	assert.Equal(t, "HIGH", update.Priority)
	assert.Equal(t, "user", update.Author)
}

func TestConsumer_InvalidJSON_DoesNotCrash(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	const rawTopic = "test-agent-invalid-json"
	const processedTopic = "test-agent-invalid-json-processed"
	createTopic(t, brokers, rawTopic)
	createTopic(t, brokers, processedTopic)

	producer := kafkaproducer.NewProducer(brokers, processedTopic)
	t.Cleanup(func() { _ = producer.Close() })
	grouper := agentservice.NewGrouper(50*time.Millisecond, producer)
	t.Cleanup(func() { _ = grouper.Close(context.Background()) })

	processor := agentservice.NewProcessor(agentservice.Config{
		Filtering: agentservice.FilteringConfig{
			MinLength: 1,
		},
		Summarization: agentservice.SummarizationConfig{
			Threshold: 500,
		},
	}, agentservice.NewStubSummarizer(), grouper)

	consumer := kafkaconsumer.NewConsumer(brokers, rawTopic, "cg-agent-invalid", processor, noopLogger())
	t.Cleanup(func() { _ = consumer.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.Start(ctx)

	writeMsg(t, brokers, rawTopic, []byte("not json at all {{{"))

	_, ok := readProcessed(t, brokers, processedTopic, 3*time.Second)
	assert.False(t, ok, "невалидный JSON не должен публиковаться в processed topic")
}

func TestConsumer_FilteredMessage_NotPublished(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	const rawTopic = "test-agent-filtered"
	const processedTopic = "test-agent-filtered-processed"
	createTopic(t, brokers, rawTopic)
	createTopic(t, brokers, processedTopic)

	producer := kafkaproducer.NewProducer(brokers, processedTopic)
	t.Cleanup(func() { _ = producer.Close() })
	grouper := agentservice.NewGrouper(50*time.Millisecond, producer)
	t.Cleanup(func() { _ = grouper.Close(context.Background()) })

	processor := agentservice.NewProcessor(agentservice.Config{
		Filtering: agentservice.FilteringConfig{
			StopWords: []string{"spam"},
			MinLength: 1,
		},
		Summarization: agentservice.SummarizationConfig{
			Threshold: 500,
		},
	}, agentservice.NewStubSummarizer(), grouper)

	consumer := kafkaconsumer.NewConsumer(brokers, rawTopic, "cg-agent-filtered", processor, noopLogger())
	t.Cleanup(func() { _ = consumer.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.Start(ctx)

	body, _ := json.Marshal(pkgdomain.LinkUpdate{
		ID:          2,
		URL:         "https://github.com/x/z",
		Author:      "user",
		Description: "this message contains spam and should be filtered",
		TgChatIDs:   []int64{1},
	})
	writeMsg(t, brokers, rawTopic, body)

	_, ok := readProcessed(t, brokers, processedTopic, 3*time.Second)
	assert.False(t, ok, "отфильтрованное сообщение не должно попадать в processed topic")
}
