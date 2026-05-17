package service

import "context"

const ellipsisLength = 3

// StubSummarizer truncates text to the configured threshold.
type StubSummarizer struct{}

// NewStubSummarizer creates a stub summarizer implementation.
func NewStubSummarizer() *StubSummarizer {
	return &StubSummarizer{}
}

// Summarize shortens the text and appends ellipsis when needed.
func (s *StubSummarizer) Summarize(_ context.Context, text string, threshold int) (string, error) {
	runes := []rune(text)
	if threshold <= 0 || len(runes) <= threshold {
		return text, nil
	}

	if threshold <= ellipsisLength {
		return string(runes[:threshold]), nil
	}

	return string(runes[:threshold-ellipsisLength]) + "...", nil
}
