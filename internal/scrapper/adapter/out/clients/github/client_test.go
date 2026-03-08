package github_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/clients/github"
)

func TestGitHubClient_NonOKStatus_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := github.NewClientWithBaseURL("", srv.URL)

	_, err := client.GetLastUpdated(context.Background(), "https://github.com/user/repo")

	require.Error(t, err)
	assert.NotPanics(t, func() {
		_, _ = client.GetLastUpdated(context.Background(), "https://github.com/user/repo")
	})
}

func TestGitHubClient_InvalidBody_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	client := github.NewClientWithBaseURL("", srv.URL)

	_, err := client.GetLastUpdated(context.Background(), "https://github.com/user/repo")

	require.Error(t, err)
}
