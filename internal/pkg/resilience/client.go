package resilience

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	retry "github.com/avast/retry-go/v4"
	"github.com/sony/gobreaker"
)

type Config struct {
	Timeout           time.Duration
	RetryAttempts     uint
	RetryDelay        time.Duration
	RetryableStatuses []int
	CBName            string
	CBMinCalls        uint32
	CBFailureRate     float64
	CBHalfOpenCalls   uint32
	CBWaitDuration    time.Duration
}

type statusError struct{ code int }

func (e *statusError) Error() string { return fmt.Sprintf("HTTP %d", e.code) }

type Client struct {
	cb  *gobreaker.CircuitBreaker
	cfg Config
}

func NewClient(cfg Config) *Client {
	const percentMultiplier = 100.0
	settings := gobreaker.Settings{
		Name:        cfg.CBName,
		MaxRequests: cfg.CBHalfOpenCalls,
		Timeout:     cfg.CBWaitDuration,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.Requests < cfg.CBMinCalls {
				return false
			}
			rate := float64(counts.TotalFailures) / float64(counts.Requests) * percentMultiplier
			return rate >= cfg.CBFailureRate
		},
	}
	return &Client{cb: gobreaker.NewCircuitBreaker(settings), cfg: cfg}
}

func (c *Client) Execute(ctx context.Context, fn func(ctx context.Context) (*http.Response, error)) (*http.Response, error) {
	var resp *http.Response

	err := retry.Do(
		func() error {
			attemptCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
			defer cancel()

			result, cbErr := c.cb.Execute(func() (any, error) {
				r, doErr := fn(attemptCtx)
				if doErr != nil {
					return nil, doErr
				}
				if r.StatusCode >= http.StatusInternalServerError {
					_ = r.Body.Close()
					return nil, &statusError{code: r.StatusCode}
				}
				return r, nil
			})

			if cbErr != nil {
				if errors.Is(cbErr, gobreaker.ErrOpenState) ||
					errors.Is(cbErr, gobreaker.ErrTooManyRequests) {
					return retry.Unrecoverable(fmt.Errorf("circuit breaker: %w", cbErr))
				}
				var se *statusError
				if errors.As(cbErr, &se) && !c.isRetryable(se.code) {
					return retry.Unrecoverable(fmt.Errorf("non-retryable status: %w", cbErr))
				}
				return fmt.Errorf("cb execution: %w", cbErr)
			}

			var ok bool
			resp, ok = result.(*http.Response)
			if !ok {
				return retry.Unrecoverable(errors.New("result is not *http.Response"))
			}
			return nil
		},
		retry.Attempts(c.cfg.RetryAttempts),
		retry.Delay(c.cfg.RetryDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
		retry.Context(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("resilient execution failed: %w", err)
	}

	return resp, nil
}

func (c *Client) State() gobreaker.State { return c.cb.State() }

func (c *Client) isRetryable(code int) bool {
	return slices.Contains(c.cfg.RetryableStatuses, code)
}
