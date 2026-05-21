//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafka "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/integration"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

type mockSvc struct {
	count     atomic.Int32
	failTimes int
	err       error
}

func (m *mockSvc) SendUpdate(_ context.Context, _ pkgdomain.LinkUpdate) error {
	n := int(m.count.Add(1))
	if m.failTimes > 0 && n <= m.failTimes {
		return m.err
	}
	return nil
}

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
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

func readFromDLQ(t *testing.T, brokers []string, dlqTopic string, timeout time.Duration) (kafkago.Message, bool) {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       dlqTopic,
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
		return kafkago.Message{}, false
	}
	return msg, true
}

func TestConsumer_ValidMessage_Processed(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	topic, dlq := uniqueName("test-consumer-valid"), uniqueName("test-consumer-valid-dlq")
	createTopic(t, brokers, topic)
	createTopic(t, brokers, dlq)

	svc := &mockSvc{}
	c := kafka.NewConsumer(brokers, topic, uniqueName("cg-valid"), dlq, 3, svc, noopLogger())
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	update := pkgdomain.LinkUpdate{ID: 1, URL: "https://github.com/x/y", TgChatIDs: []int64{1}}
	body, _ := json.Marshal(update)
	writeMsg(t, brokers, topic, body)

	assert.Eventually(t, func() bool {
		return svc.count.Load() >= 1
	}, 15*time.Second, 200*time.Millisecond)
}

func TestConsumer_InvalidJSON_SentToDLQ(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	topic, dlq := uniqueName("test-consumer-badjson"), uniqueName("test-consumer-badjson-dlq")
	createTopic(t, brokers, topic)
	createTopic(t, brokers, dlq)

	svc := &mockSvc{}
	c := kafka.NewConsumer(brokers, topic, uniqueName("cg-badjson"), dlq, 3, svc, noopLogger())
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	writeMsg(t, brokers, topic, []byte("not json at all {{{"))

	// По требованиям: ошибка десериализации → сразу в DLQ
	dlqMsg, ok := readFromDLQ(t, brokers, dlq, 10*time.Second)
	require.True(t, ok, "невалидный JSON должен попасть в DLQ")
	assert.Equal(t, []byte("not json at all {{{"), dlqMsg.Value)
	assert.Equal(t, int32(0), svc.count.Load(), "UpdateService не должен быть вызван")
}

func TestConsumer_ValidationError_SentToDLQ(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	topic, dlq := uniqueName("test-consumer-validation"), uniqueName("test-consumer-validation-dlq")
	createTopic(t, brokers, topic)
	createTopic(t, brokers, dlq)

	svc := &mockSvc{}
	c := kafka.NewConsumer(brokers, topic, uniqueName("cg-validation"), dlq, 3, svc, noopLogger())
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	// URL пустой → ошибка валидации
	invalid := pkgdomain.LinkUpdate{ID: 1, TgChatIDs: []int64{1}}
	body, _ := json.Marshal(invalid)
	writeMsg(t, brokers, topic, body)

	// По требованиям: ошибка валидации → сразу в DLQ
	dlqMsg, ok := readFromDLQ(t, brokers, dlq, 10*time.Second)
	require.True(t, ok, "невалидное сообщение должно попасть в DLQ")
	assert.Equal(t, body, dlqMsg.Value)
	assert.Equal(t, int32(0), svc.count.Load())
}

func TestConsumer_BusinessError_RetriedThenDLQ(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	topic, dlq := uniqueName("test-consumer-retry-dlq"), uniqueName("test-consumer-retry-dlq-dead")
	createTopic(t, brokers, topic)
	createTopic(t, brokers, dlq)
	const maxRetries = 2

	svc := &mockSvc{
		err:       errors.New("telegram api down"),
		failTimes: maxRetries + 1,
	}
	c := kafka.NewConsumer(brokers, topic, uniqueName("cg-retry-dlq"), dlq, maxRetries, svc, noopLogger())
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	update := pkgdomain.LinkUpdate{ID: 2, URL: "https://github.com/x/y", TgChatIDs: []int64{1}}
	body, _ := json.Marshal(update)
	writeMsg(t, brokers, topic, body)

	dlqMsg, ok := readFromDLQ(t, brokers, dlq, 10*time.Second)
	require.True(t, ok, "сообщение должно попасть в DLQ после исчерпания ретраев")
	assert.Equal(t, body, dlqMsg.Value)

	var hasErrorHeader bool
	for _, h := range dlqMsg.Headers {
		if h.Key == "error" {
			hasErrorHeader = true
			break
		}
	}
	assert.True(t, hasErrorHeader, "DLQ сообщение должно содержать хедер error")
	assert.Equal(t, int32(maxRetries+1), svc.count.Load())
}

func TestConsumer_BusinessError_SucceedsOnRetry(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	topic, dlq := uniqueName("test-consumer-retry-ok"), uniqueName("test-consumer-retry-ok-dead")
	createTopic(t, brokers, topic)
	createTopic(t, brokers, dlq)

	svc := &mockSvc{
		err:       errors.New("temporary error"),
		failTimes: 1,
	}
	c := kafka.NewConsumer(brokers, topic, uniqueName("cg-retry-ok"), dlq, 3, svc, noopLogger())
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	update := pkgdomain.LinkUpdate{ID: 3, URL: "https://github.com/x/z", TgChatIDs: []int64{1}}
	body, _ := json.Marshal(update)
	writeMsg(t, brokers, topic, body)

	assert.Eventually(t, func() bool {
		return svc.count.Load() >= 2
	}, 15*time.Second, 300*time.Millisecond)

	_, hasDLQ := readFromDLQ(t, brokers, dlq, 2*time.Second)
	assert.False(t, hasDLQ, "успешно обработанное сообщение не должно попасть в DLQ")
}
