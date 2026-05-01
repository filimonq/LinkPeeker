//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	kafkaproducer "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/outbox"
	pgxrepo "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/pgx"
)

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func startPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	pgc, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("outbox_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgc.Terminate(ctx) })

	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS outbox (
			id         BIGSERIAL PRIMARY KEY,
			payload    JSONB       NOT NULL,
			status     TEXT        NOT NULL DEFAULT 'pending',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			sent_at    TIMESTAMPTZ
		)`)
	require.NoError(t, err)

	return pool
}

func startKafka(t *testing.T, ctx context.Context) []string {
	t.Helper()
	kc, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)
	return brokers
}

func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	require.NoError(t, err)
	defer conn.Close()
	err = conn.CreateTopics(kafkago.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	})
	require.NoError(t, err)
}

func TestOutbox_PollerDeliversPendingEntriesToKafka(t *testing.T) {
	ctx := context.Background()

	pool := startPostgres(t, ctx)
	brokers := startKafka(t, ctx)
	const topic = "outbox-test-deliver"
	createTopic(t, brokers, topic)

	outboxRepo := pgxrepo.NewOutboxRepository(pool)
	producer := kafkaproducer.NewProducer(brokers, topic)
	t.Cleanup(func() { _ = producer.Close() })

	update := pkgdomain.LinkUpdate{
		ID: 1, URL: "https://github.com/user/repo",
		Description: "new commit", TgChatIDs: []int64{10, 20},
	}
	payload, err := json.Marshal(update)
	require.NoError(t, err)

	require.NoError(t, outboxRepo.Add(ctx, payload))

	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM outbox WHERE id = 1`).Scan(&status))
	assert.Equal(t, "pending", status)

	poller := outbox.NewPoller(outboxRepo, producer, time.Hour, 10, noopLogger())
	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go poller.Start(pollCtx)

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, Partition: 0,
		MinBytes: 1, MaxBytes: 1024 * 1024,
	})
	t.Cleanup(func() { _ = reader.Close() })

	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()
	msg, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)

	var got pkgdomain.LinkUpdate
	require.NoError(t, json.Unmarshal(msg.Value, &got))
	assert.Equal(t, update.URL, got.URL)
	assert.Equal(t, update.TgChatIDs, got.TgChatIDs)

	assert.Eventually(t, func() bool {
		var s string
		_ = pool.QueryRow(ctx, `SELECT status FROM outbox WHERE id = 1`).Scan(&s)
		return s == "sent"
	}, 10*time.Second, 200*time.Millisecond, "outbox row should be marked 'sent' after delivery")
}

func TestOutbox_RowRemainsOnPublishFailure(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t, ctx)
	outboxRepo := pgxrepo.NewOutboxRepository(pool)

	update := pkgdomain.LinkUpdate{ID: 2, URL: "https://github.com/x/y", TgChatIDs: []int64{1}}
	payload, _ := json.Marshal(update)
	require.NoError(t, outboxRepo.Add(ctx, payload))

	failingProducer := kafkaproducer.NewProducer([]string{"localhost:1"}, "no-topic")
	t.Cleanup(func() { _ = failingProducer.Close() })

	poller := outbox.NewPoller(outboxRepo, failingProducer, time.Hour, 10, noopLogger())
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go poller.Start(pollCtx)
	<-pollCtx.Done()

	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM outbox WHERE id = 1`).Scan(&status))
	assert.Equal(t, "pending", status, "row must stay pending when publish fails")
}

func TestOutbox_RollbackLeavesOutboxEmpty(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t, ctx)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	payload, _ := json.Marshal(pkgdomain.LinkUpdate{ID: 3, URL: "https://x.com", TgChatIDs: []int64{1}})
	_, err = tx.Exec(ctx, `INSERT INTO outbox (payload) VALUES ($1)`, payload)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))

	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&count))
	assert.Equal(t, 0, count, "rolled-back tx must leave outbox empty")
}

func TestOutbox_CommitPersistsEntry(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t, ctx)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	payload, _ := json.Marshal(pkgdomain.LinkUpdate{ID: 4, URL: "https://y.com", TgChatIDs: []int64{2}})
	_, err = tx.Exec(ctx, `INSERT INTO outbox (payload) VALUES ($1)`, payload)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&count))
	assert.Equal(t, 1, count, "committed tx must persist the outbox entry")
}
