package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/middleware"
)

func TestRateLimiter_Returns429WhenLimitExceeded(t *testing.T) {
	const burst = 2

	handler := middleware.PerIPRateLimiter(2.0, burst)(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := &http.Client{}

	for range burst {
		resp, err := client.Get(srv.URL)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	resp.Body.Close()
}

func TestRateLimiter_DifferentIPsAreIsolated(t *testing.T) {
	handler := middleware.PerIPRateLimiter(1.0, 1)(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	srv := httptest.NewServer(handler)
	defer srv.Close()

	makeRequest := func(ip string) int {
		req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		req.Header.Set("X-Real-IP", ip)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}

	assert.Equal(t, http.StatusOK, makeRequest("10.0.0.1"))
	assert.Equal(t, http.StatusTooManyRequests, makeRequest("10.0.0.1"))
	assert.Equal(t, http.StatusOK, makeRequest("10.0.0.2"))
}
