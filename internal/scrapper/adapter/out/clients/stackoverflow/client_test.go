package stackoverflow_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/stackoverflow"
)

func TestSOClient_NonOKStatus_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	client := stackoverflow.NewClientWithBaseURL(srv.URL)

	_, err := client.GetLastUpdated(context.Background(), "https://stackoverflow.com/questions/123/test")

	require.Error(t, err)
	assert.NotPanics(t, func() {
		_, _ = client.GetLastUpdated(context.Background(), "https://stackoverflow.com/questions/123/test")
	})
}

func TestSOClient_InvalidBody_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	client := stackoverflow.NewClientWithBaseURL(srv.URL)

	_, err := client.GetLastUpdated(context.Background(), "https://stackoverflow.com/questions/123/test")

	require.Error(t, err)
}
