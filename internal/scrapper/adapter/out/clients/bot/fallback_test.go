package bot_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	botclient "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/bot"
)

type mockNotifier struct {
	err    error
	called bool
}

func (m *mockNotifier) SendUpdate(_ context.Context, _ pkgdomain.LinkUpdate) error {
	m.called = true
	return m.err
}

func TestFallbackNotifier_UsesFallbackWhenPrimaryFails(t *testing.T) {
	primary := &mockNotifier{err: errors.New("http down")}
	fallback := &mockNotifier{}

	fn := botclient.NewFallbackNotifier(primary, fallback, slog.Default())
	err := fn.SendUpdate(context.Background(), pkgdomain.LinkUpdate{})

	require.NoError(t, err)
	assert.True(t, primary.called)
	assert.True(t, fallback.called)
}

func TestFallbackNotifier_SkipsFallbackWhenPrimarySucceeds(t *testing.T) {
	primary := &mockNotifier{}
	fallback := &mockNotifier{}

	fn := botclient.NewFallbackNotifier(primary, fallback, slog.Default())
	err := fn.SendUpdate(context.Background(), pkgdomain.LinkUpdate{})

	require.NoError(t, err)
	assert.True(t, primary.called)
	assert.False(t, fallback.called)
}

func TestFallbackNotifier_ReturnsFallbackErrorWhenBothFail(t *testing.T) {
	primary := &mockNotifier{err: errors.New("http down")}
	fallback := &mockNotifier{err: errors.New("kafka down")}

	fn := botclient.NewFallbackNotifier(primary, fallback, slog.Default())
	err := fn.SendUpdate(context.Background(), pkgdomain.LinkUpdate{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka down")
	assert.True(t, primary.called)
	assert.True(t, fallback.called)
}
