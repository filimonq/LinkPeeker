//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/integration"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	kafka "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/kafka"
)

func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	require.NoError(t, err)
	defer conn.Close()

	err = conn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		partitions, pErr := conn.ReadPartitions(topic)
		return pErr == nil && len(partitions) > 0
	}, 10*time.Second, 100*time.Millisecond, "topic %s was not created in time", topic)
}

func readOne(t *testing.T, brokers []string, topic string) kafkago.Message {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10 * 1024 * 1024,
	})
	t.Cleanup(func() { _ = r.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	msg, err := r.ReadMessage(ctx)
	require.NoError(t, err)
	return msg
}

func TestProducer_SendUpdate_MessageLandsInTopic(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	const topic = "test-producer-lands"
	createTopic(t, brokers, topic)

	p := kafka.NewProducer(brokers, topic)
	t.Cleanup(func() { _ = p.Close() })

	update := pkgdomain.LinkUpdate{
		ID:          42,
		URL:         "https://github.com/user/repo",
		Description: "new commit",
		TgChatIDs:   []int64{1, 2},
	}

	require.NoError(t, p.SendUpdate(context.Background(), update))

	msg := readOne(t, brokers, topic)

	var got pkgdomain.LinkUpdate
	require.NoError(t, json.Unmarshal(msg.Value, &got))

	assert.Equal(t, update.ID, got.ID)
	assert.Equal(t, update.URL, got.URL)
	assert.Equal(t, update.Description, got.Description)
	assert.Equal(t, update.TgChatIDs, got.TgChatIDs)
}

func TestProducer_SendUpdate_KeyIsLinkID(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	const topic = "test-producer-key"
	createTopic(t, brokers, topic)

	p := kafka.NewProducer(brokers, topic)
	t.Cleanup(func() { _ = p.Close() })

	update := pkgdomain.LinkUpdate{ID: 99, URL: "https://github.com/x/y", TgChatIDs: []int64{1}}
	assert.Eventually(t, func() bool {
		return p.SendUpdate(context.Background(), update) == nil
	}, 10*time.Second, 500*time.Millisecond)

	msg := readOne(t, brokers, topic)
	assert.Equal(t, strconv.FormatInt(update.ID, 10), string(msg.Key))
}

func TestProducer_SendUpdate_MultipleMessages(t *testing.T) {
	brokers := integration.GetTestKafka(t)
	const topic = "test-producer-multi"
	createTopic(t, brokers, topic)

	p := kafka.NewProducer(brokers, topic)
	t.Cleanup(func() { _ = p.Close() })

	updates := []pkgdomain.LinkUpdate{
		{ID: 1, URL: "https://github.com/a/b", TgChatIDs: []int64{1}},
		{ID: 2, URL: "https://github.com/c/d", TgChatIDs: []int64{2}},
		{ID: 3, URL: "https://github.com/e/f", TgChatIDs: []int64{3}},
	}

	ctx := context.Background()
	for _, u := range updates {
		u := u
		require.Eventually(t, func() bool {
			return p.SendUpdate(ctx, u) == nil
		}, 10*time.Second, 500*time.Millisecond, "cannot send update %d", u.ID)
	}

	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, Partition: 0,
		MinBytes: 1, MaxBytes: 10 * 1024 * 1024,
	})
	t.Cleanup(func() { _ = r.Close() })

	received := make(map[int64]bool)
	for range updates {
		readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		msg, err := r.ReadMessage(readCtx)
		cancel()
		require.NoError(t, err)

		var got pkgdomain.LinkUpdate
		require.NoError(t, json.Unmarshal(msg.Value, &got))
		received[got.ID] = true
	}

	for _, u := range updates {
		assert.True(t, received[u.ID], "ID %d должен быть получен", u.ID)
	}
}
