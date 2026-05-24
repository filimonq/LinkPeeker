package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/resilience"
)

const defaultOpenAIBaseURL = "https://api.openai.com/v1"

const (
	defaultOpenAITimeout       = 15 * time.Second
	defaultOpenAIRetryAttempts = 3
	defaultOpenAIRetryDelay    = 500 * time.Millisecond
	defaultOpenAICBWindowSize  = 10
	defaultOpenAICBHalfOpen    = 5
	defaultOpenAIMinCalls      = 5
	defaultOpenAIFailureRate   = 50.0
)

// OpenAISummarizer calls an OpenAI-compatible chat completions API.
type OpenAISummarizer struct {
	http    *http.Client
	rc      *resilience.Client
	baseURL string
	apiKey  string
	model   string
	prompt  string
}

type openAIChatRequest struct {
	Model    string              `json:"model"`
	Messages []openAIChatMessage `json:"messages"`
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatResponse struct {
	Choices []struct {
		Message openAIChatMessage `json:"message"`
	} `json:"choices"`
}

// NewOpenAISummarizer creates a summarizer backed by an OpenAI-compatible API.
func NewOpenAISummarizer(
	baseURL, apiKey, model, prompt string,
	timeout time.Duration,
	rc *resilience.Client,
) *OpenAISummarizer {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultOpenAIBaseURL
	}
	if timeout <= 0 {
		timeout = defaultOpenAITimeout
	}
	if rc == nil {
		rc = newDefaultResilientClient("openai-summarizer")
	}

	return &OpenAISummarizer{
		http: &http.Client{
			Timeout: timeout,
		},
		rc:      rc,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		prompt:  prompt,
	}
}

func newOpenAISummarizerWithHTTPClient(
	httpClient *http.Client,
	rc *resilience.Client,
	baseURL, apiKey, model, prompt string,
) *OpenAISummarizer {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultOpenAIBaseURL
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultOpenAITimeout}
	}
	if rc == nil {
		rc = newDefaultResilientClient("openai-summarizer-test")
	}

	return &OpenAISummarizer{
		http:    httpClient,
		rc:      rc,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		prompt:  prompt,
	}
}

// Summarize shortens text using a remote LLM and trims the response to threshold.
func (s *OpenAISummarizer) Summarize(ctx context.Context, text string, threshold int) (string, error) {
	if threshold <= 0 || len([]rune(text)) <= threshold {
		return text, nil
	}
	if strings.TrimSpace(s.apiKey) == "" {
		return "", errors.New("openai summarizer api key is empty")
	}
	if strings.TrimSpace(s.model) == "" {
		return "", errors.New("openai summarizer model is empty")
	}

	reqBody := openAIChatRequest{
		Model: s.model,
		Messages: []openAIChatMessage{
			{
				Role:    "system",
				Content: strings.TrimSpace(s.prompt),
			},
			{
				Role:    "user",
				Content: text,
			},
		},
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal chat request: %w", err)
	}

	resp, err := s.rc.Execute(ctx, func(ctx context.Context) (*http.Response, error) {
		req, reqErr := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			s.baseURL+"/chat/completions",
			bytes.NewReader(payload),
		)
		if reqErr != nil {
			return nil, fmt.Errorf("create chat request: %w", reqErr)
		}

		req.Header.Set("Authorization", "Bearer "+s.apiKey)
		req.Header.Set("Content-Type", "application/json")

		return s.http.Do(req)
	})
	if err != nil {
		return "", fmt.Errorf("call summarization api: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("summarization api status: %d", resp.StatusCode)
	}

	var decoded openAIChatResponse
	if err = json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", fmt.Errorf("decode chat response: %w", err)
	}
	if len(decoded.Choices) == 0 {
		return "", errors.New("summarization api returned no choices")
	}

	summary := strings.TrimSpace(decoded.Choices[0].Message.Content)
	if summary == "" {
		return "", errors.New("summarization api returned empty summary")
	}

	return NewStubSummarizer().Summarize(ctx, summary, threshold)
}

func newDefaultResilientClient(name string) *resilience.Client {
	return resilience.NewClient(resilience.Config{
		Timeout:               defaultOpenAITimeout,
		RetryAttempts:         defaultOpenAIRetryAttempts,
		RetryDelay:            defaultOpenAIRetryDelay,
		RetryableStatuses:     []int{500, 502, 503, 504},
		UseExponentialBackoff: true,
		CBName:                name,
		CBSlidingWindowSize:   defaultOpenAICBWindowSize,
		CBMinCalls:            defaultOpenAIMinCalls,
		CBFailureRate:         defaultOpenAIFailureRate,
		CBHalfOpenCalls:       defaultOpenAICBHalfOpen,
		CBWaitDuration:        time.Second,
	})
}
