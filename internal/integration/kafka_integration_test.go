package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"

	kafkaconsumer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/kafka"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	kafkaproducer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/kafka"
)

type countingUpdateService struct {
	count   atomic.Int32
	updates []pkgdomain.LinkUpdate
}

func (s *countingUpdateService) SendUpdate(_ context.Context, u pkgdomain.LinkUpdate) error {
	s.updates = append(s.updates, u)
	s.count.Add(1)
	return nil
}

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestKafka_ScrapperToBot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	kc, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	const topic = "link-updates-e2e"
	const dlq = "link-updates-e2e-dlq"

	conn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	require.NoError(t, err)
	_ = conn.Close()

	producer := kafkaproducer.NewProducer(brokers, topic)
	t.Cleanup(func() { _ = producer.Close() })

	svc := &countingUpdateService{}
	consumer := kafkaconsumer.NewConsumer(
		brokers, topic, "e2e-group", dlq, 3, svc, noopLogger(),
	)
	t.Cleanup(func() { _ = consumer.Close() })

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()
	go consumer.Start(consumerCtx)

	updates := []pkgdomain.LinkUpdate{
		{ID: 1, URL: "https://github.com/user/repo1", Description: "update 1", TgChatIDs: []int64{1}},
		{ID: 2, URL: "https://github.com/user/repo2", Description: "update 2", TgChatIDs: []int64{2}},
		{ID: 3, URL: "https://github.com/user/repo3", Description: "update 3", TgChatIDs: []int64{1, 2}},
	}

	for _, u := range updates {
		require.NoError(t, producer.SendUpdate(ctx, u))
	}

	assert.Eventually(t, func() bool {
		return svc.count.Load() == int32(len(updates))
	}, 20*time.Second, 300*time.Millisecond, "bot должен обработать все обновления")

	cancelConsumer()

	receivedURLs := make(map[string]bool)
	for _, u := range svc.updates {
		receivedURLs[u.URL] = true
	}
	for _, u := range updates {
		assert.True(t, receivedURLs[u.URL], "URL %s должен быть получен ботом", u.URL)
	}
}

func TestKafka_ProducerImplementsBotNotifier(_ *testing.T) {
	var _ interface {
		SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error
	} = kafkaproducer.NewProducer([]string{"localhost:9092"}, "test")
}

func TestKafka_MessageFormat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	kc, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	const topic = "link-updates-format"

	conn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	require.NoError(t, err)
	_ = conn.Close()

	producer := kafkaproducer.NewProducer(brokers, topic)
	t.Cleanup(func() { _ = producer.Close() })

	update := pkgdomain.LinkUpdate{
		ID:          7,
		URL:         "https://stackoverflow.com/questions/123",
		Description: "new answer",
		TgChatIDs:   []int64{10, 20},
	}
	require.NoError(t, producer.SendUpdate(ctx, update))

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, Partition: 0,
		MinBytes: 1, MaxBytes: 1024 * 1024,
	})
	t.Cleanup(func() { _ = reader.Close() })

	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)

	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(msg.Value, &raw))

	assert.Equal(t, strconv.FormatInt(update.ID, 10), fmt.Sprintf("%.0f", raw["id"]))
	assert.Equal(t, update.URL, raw["url"])
	assert.Equal(t, update.Description, raw["description"])

	chatIDs, ok := raw["tgChatIds"].([]interface{})
	require.True(t, ok)
	assert.Len(t, chatIDs, 2)
}
