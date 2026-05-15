package resilience_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/resilience"
)

func testConfig(name string) resilience.Config {
	return resilience.Config{
		Timeout:           5 * time.Second,
		RetryAttempts:     1,
		RetryDelay:        0,
		RetryableStatuses: []int{},
		CBName:            name,
		CBMinCalls:        1000,
		CBFailureRate:     100,
		CBHalfOpenCalls:   1,
		CBWaitDuration:    time.Second,
	}
}

func doGet(t *testing.T, client *resilience.Client, url string) (*http.Response, error) {
	t.Helper()
	return client.Execute(context.Background(), func(ctx context.Context) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		require.NoError(t, err)
		return http.DefaultClient.Do(req)
	})
}

func TestTimeout_RequestExceedsDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testConfig("timeout-test")
	cfg.Timeout = 50 * time.Millisecond
	client := resilience.NewClient(cfg)

	start := time.Now()
	_, err := doGet(t, client, srv.URL)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, 200*time.Millisecond)
}

func TestRetry_RetriesOnRetryableStatus(t *testing.T) {
	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := callCount.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := testConfig("retry-5xx")
	cfg.RetryAttempts = 3
	cfg.RetryDelay = 10 * time.Millisecond
	cfg.RetryableStatuses = []int{500}
	client := resilience.NewClient(cfg)

	resp, err := doGet(t, client, srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(3), callCount.Load())
}

func TestRetry_NoRetryOnNonRetryableStatus(t *testing.T) {
	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	cfg := testConfig("retry-4xx")
	cfg.RetryAttempts = 3
	cfg.RetryDelay = 10 * time.Millisecond
	cfg.RetryableStatuses = []int{500}
	client := resilience.NewClient(cfg)

	resp, err := doGet(t, client, srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, int32(1), callCount.Load())
}

func TestRetry_ExponentialBackoffIntervals(t *testing.T) {
	var (
		mu         sync.Mutex
		timestamps []time.Time
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		timestamps = append(timestamps, time.Now())
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := testConfig("backoff-test")
	cfg.RetryAttempts = 3
	cfg.RetryDelay = 50 * time.Millisecond
	cfg.RetryableStatuses = []int{500}
	client := resilience.NewClient(cfg)

	_, _ = doGet(t, client, srv.URL)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, timestamps, 3)

	interval1 := timestamps[1].Sub(timestamps[0])
	interval2 := timestamps[2].Sub(timestamps[1])

	assert.GreaterOrEqual(t, interval2, interval1)
	assert.GreaterOrEqual(t, interval1, 25*time.Millisecond)
}

func TestCircuitBreaker_TransitionsToOpen(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := testConfig("cb-open")
	cfg.CBMinCalls = 3
	cfg.CBFailureRate = 100
	cfg.CBWaitDuration = 10 * time.Second
	cfg.RetryAttempts = 1
	client := resilience.NewClient(cfg)

	for range 3 {
		_, _ = doGet(t, client, srv.URL)
	}

	assert.Equal(t, gobreaker.StateOpen, client.State())

	start := time.Now()
	_, err := doGet(t, client, srv.URL)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, 50*time.Millisecond)
}

func TestCircuitBreaker_HalfOpen_TransitionsToClosed(t *testing.T) {
	const waitDuration = 100 * time.Millisecond
	const halfOpenCalls = uint32(2)

	shouldFail := atomic.Bool{}
	shouldFail.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if shouldFail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := testConfig("cb-halfopen-closed")
	cfg.CBMinCalls = 3
	cfg.CBFailureRate = 100
	cfg.CBWaitDuration = waitDuration
	cfg.CBHalfOpenCalls = halfOpenCalls
	cfg.RetryAttempts = 1
	client := resilience.NewClient(cfg)

	for range 3 {
		_, _ = doGet(t, client, srv.URL)
	}
	require.Equal(t, gobreaker.StateOpen, client.State())

	time.Sleep(waitDuration + 20*time.Millisecond)
	shouldFail.Store(false)

	for range int(halfOpenCalls) {
		resp, err := doGet(t, client, srv.URL)
		require.NoError(t, err)
		resp.Body.Close()
	}

	assert.Equal(t, gobreaker.StateClosed, client.State())
}

func TestCircuitBreaker_HalfOpen_TransitionsBackToOpen(t *testing.T) {
	const waitDuration = 100 * time.Millisecond

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := testConfig("cb-halfopen-open")
	cfg.CBMinCalls = 3
	cfg.CBFailureRate = 100
	cfg.CBWaitDuration = waitDuration
	cfg.CBHalfOpenCalls = 2
	cfg.RetryAttempts = 1
	client := resilience.NewClient(cfg)

	for range 3 {
		_, _ = doGet(t, client, srv.URL)
	}
	require.Equal(t, gobreaker.StateOpen, client.State())

	time.Sleep(waitDuration + 20*time.Millisecond)

	_, _ = doGet(t, client, srv.URL)

	assert.Equal(t, gobreaker.StateOpen, client.State())
}
