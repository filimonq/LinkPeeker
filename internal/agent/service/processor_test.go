package service_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/service"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

type mockPublisher struct {
	updates []pkgdomain.LinkUpdate
	err     error
}

func (m *mockPublisher) SendUpdate(_ context.Context, update pkgdomain.LinkUpdate) error {
	if m.err != nil {
		return m.err
	}
	m.updates = append(m.updates, update)
	return nil
}

func newProcessor(publisher *mockPublisher) *service.Processor {
	return service.NewProcessor(service.Config{
		Filtering: service.FilteringConfig{
			StopWords:       []string{"spam", "promo"},
			ExcludedAuthors: []string{"bot-user"},
			MinLength:       20,
		},
		Summarization: service.SummarizationConfig{
			Threshold: 40,
		},
	}, service.NewStubSummarizer(), publisher)
}

func TestProcessor_FiltersByStopWord(t *testing.T) {
	publisher := &mockPublisher{}
	processor := newProcessor(publisher)

	err := processor.Process(context.Background(), pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://example.com",
		Author:      "user",
		Description: "This update contains spam inside the body",
		TgChatIDs:   []int64{1},
	})

	require.NoError(t, err)
	assert.Empty(t, publisher.updates)
}

func TestProcessor_FiltersByAuthor(t *testing.T) {
	publisher := &mockPublisher{}
	processor := newProcessor(publisher)

	err := processor.Process(context.Background(), pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://example.com",
		Author:      "bot-user",
		Description: "This update text is definitely long enough",
		TgChatIDs:   []int64{1},
	})

	require.NoError(t, err)
	assert.Empty(t, publisher.updates)
}

func TestProcessor_FiltersByMinLength(t *testing.T) {
	publisher := &mockPublisher{}
	processor := newProcessor(publisher)

	err := processor.Process(context.Background(), pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://example.com",
		Author:      "user",
		Description: "too short",
		TgChatIDs:   []int64{1},
	})

	require.NoError(t, err)
	assert.Empty(t, publisher.updates)
}

func TestProcessor_PublishesValidUpdate(t *testing.T) {
	publisher := &mockPublisher{}
	processor := newProcessor(publisher)
	update := pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://example.com",
		Author:      "user",
		Description: "This update text is valid",
		TgChatIDs:   []int64{1, 2},
	}

	err := processor.Process(context.Background(), update)

	require.NoError(t, err)
	require.Len(t, publisher.updates, 1)
	assert.Equal(t, "HIGH", publisher.updates[0].Priority)
	assert.Equal(t, update.Description, publisher.updates[0].Description)
}

func TestProcessor_SummarizesLongText(t *testing.T) {
	publisher := &mockPublisher{}
	processor := newProcessor(publisher)
	longText := strings.Repeat("a", 60)

	err := processor.Process(context.Background(), pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://example.com",
		Author:      "user",
		Description: longText,
		TgChatIDs:   []int64{1},
	})

	require.NoError(t, err)
	require.Len(t, publisher.updates, 1)
	assert.Len(t, []rune(publisher.updates[0].Description), 40)
	assert.NotEqual(t, longText, publisher.updates[0].Description)
	assert.True(t, strings.HasSuffix(publisher.updates[0].Description, "..."))
}

func TestProcessor_KeepsShortTextUnchanged(t *testing.T) {
	publisher := &mockPublisher{}
	processor := newProcessor(publisher)
	text := "This update text is not too long at all"

	err := processor.Process(context.Background(), pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://example.com",
		Author:      "user",
		Description: text,
		TgChatIDs:   []int64{1},
	})

	require.NoError(t, err)
	require.Len(t, publisher.updates, 1)
	assert.Equal(t, text, publisher.updates[0].Description)
}

func TestProcessor_PropagatesPublisherError(t *testing.T) {
	publisher := &mockPublisher{err: errors.New("broker unavailable")}
	processor := newProcessor(publisher)

	err := processor.Process(context.Background(), pkgdomain.LinkUpdate{
		ID:          1,
		URL:         "https://example.com",
		Author:      "user",
		Description: "This update text is definitely long enough",
		TgChatIDs:   []int64{1},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish processed update")
}
