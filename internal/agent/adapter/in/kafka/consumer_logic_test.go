package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

type mockProcessor struct {
	err error
}

func (m *mockProcessor) Process(_ context.Context, _ pkgdomain.LinkUpdate) error {
	return m.err
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestConsumerHandle_InvalidJSON_CommitsMessage(t *testing.T) {
	consumer := &Consumer{
		processor: &mockProcessor{},
		logger:    testLogger(),
	}

	shouldCommit, err := consumer.handle(context.Background(), kafkago.Message{
		Value: []byte("not json at all {{{"),
	})

	require.NoError(t, err)
	assert.True(t, shouldCommit)
}

func TestConsumerHandle_ProcessorError_DoesNotCommitMessage(t *testing.T) {
	consumer := &Consumer{
		processor: &mockProcessor{err: errors.New("producer unavailable")},
		logger:    testLogger(),
	}

	shouldCommit, err := consumer.handle(context.Background(), kafkago.Message{
		Value: []byte(`{"id":1,"url":"https://example.com","description":"valid description","tgChatIds":[1]}`),
	})

	require.Error(t, err)
	assert.False(t, shouldCommit)
}

func TestConsumerHandle_Success_CommitsMessage(t *testing.T) {
	consumer := &Consumer{
		processor: &mockProcessor{},
		logger:    testLogger(),
	}

	shouldCommit, err := consumer.handle(context.Background(), kafkago.Message{
		Value: []byte(`{"id":1,"url":"https://example.com","description":"valid description","tgChatIds":[1]}`),
	})

	require.NoError(t, err)
	assert.True(t, shouldCommit)
}
