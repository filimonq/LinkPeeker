package bothttp_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bothttp "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/adapter/in/http"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

type mockUpdateService struct{}

func (m *mockUpdateService) SendUpdate(_ context.Context, _ pkgdomain.LinkUpdate) error {
	return nil
}

func newTestServer() *httptest.Server {
	handler := bothttp.NewHandler(&mockUpdateService{}, slog.Default())
	server := bothttp.NewServer(":0", handler)
	return httptest.NewServer(server.Handler())
}

func TestHandleUpdates_ValidBody_Returns200(t *testing.T) {
	// Arrange
	srv := newTestServer()
	defer srv.Close()
	body := `{"id":1,"url":"https://github.com/foo/bar","description":"update","tgChatIds":[123]}`

	// Act
	resp, err := http.Post(srv.URL+"/updates", "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	// Assert
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestHandleUpdates_InvalidBody_Returns400(t *testing.T) {
	// Arrange
	srv := newTestServer()
	defer srv.Close()

	// Act
	resp, err := http.Post(srv.URL+"/updates", "application/json", bytes.NewBufferString(`not json at all`))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	// Assert
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
