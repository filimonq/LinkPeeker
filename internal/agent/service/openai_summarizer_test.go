package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestOpenAISummarizer_Summarize(t *testing.T) {
	t.Parallel()

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPost, req.Method)
			assert.Equal(t, "Bearer secret", req.Header.Get("Authorization"))
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			assert.Equal(t, "https://example.com/chat/completions", req.URL.String())

			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			assert.Contains(t, string(body), `"model":"test-model"`)
			assert.Contains(t, string(body), `Summarize the following update`)

			responseBody := `{"choices":[{"message":{"content":"Short and useful summary."}}]}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	summarizer := newOpenAISummarizerWithHTTPClient(
		client,
		nil,
		"https://example.com",
		"secret",
		"test-model",
		"Summarize the following update in 2-3 sentences.",
	)

	summary, err := summarizer.Summarize(context.Background(), strings.Repeat("a", 120), 50)

	require.NoError(t, err)
	assert.Equal(t, "Short and useful summary.", summary)
}

func TestOpenAISummarizer_PropagatesHTTPError(t *testing.T) {
	t.Parallel()

	client := &http.Client{
		Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return nil, errors.New("network down")
		}),
	}

	summarizer := newOpenAISummarizerWithHTTPClient(
		client,
		nil,
		"https://example.com",
		"secret",
		"test-model",
		"prompt",
	)

	_, err := summarizer.Summarize(context.Background(), strings.Repeat("a", 120), 50)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "call summarization api")
}

func TestOpenAISummarizer_ReturnsOriginalForShortText(t *testing.T) {
	t.Parallel()

	summarizer := newOpenAISummarizerWithHTTPClient(
		&http.Client{},
		nil,
		"https://example.com",
		"secret",
		"test-model",
		"prompt",
	)

	text := "short text"
	summary, err := summarizer.Summarize(context.Background(), text, 50)

	require.NoError(t, err)
	assert.Equal(t, text, summary)
}
