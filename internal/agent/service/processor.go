package service

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

const (
	priorityHigh   = "HIGH"
	priorityMedium = "MEDIUM"
	priorityLow    = "LOW"
)

type FilteringConfig struct {
	StopWords       []string
	ExcludedAuthors []string
	MinLength       int
}

type SummarizationConfig struct {
	Threshold int
}

type PrioritizationConfig struct {
	HighKeywords []string
	LowKeywords  []string
}

type Config struct {
	Filtering      FilteringConfig
	Summarization  SummarizationConfig
	Prioritization PrioritizationConfig
}

type Processor struct {
	cfg        Config
	summarizer Summarizer
	publisher  Publisher
}

// NewProcessor creates an AI agent processor.
func NewProcessor(cfg Config, summarizer Summarizer, publisher Publisher) *Processor {
	return &Processor{
		cfg:        cfg,
		summarizer: summarizer,
		publisher:  publisher,
	}
}

// Process filters, summarizes, and publishes updates.
func (p *Processor) Process(ctx context.Context, update pkgdomain.LinkUpdate) error {
	if p.shouldDrop(update) {
		return nil
	}

	processed := update

	if p.shouldSummarize(update.Description) {
		summary, err := p.summarizer.Summarize(ctx, update.Description, p.cfg.Summarization.Threshold)
		if err != nil {
			return fmt.Errorf("summarize update: %w", err)
		}
		processed.Description = summary
	}
	processed.Priority = p.resolvePriority(processed.Description)

	if err := p.publisher.SendUpdate(ctx, processed); err != nil {
		return fmt.Errorf("publish processed update: %w", err)
	}

	return nil
}

func (p *Processor) shouldDrop(update pkgdomain.LinkUpdate) bool {
	description := strings.ToLower(update.Description)
	author := strings.ToLower(strings.TrimSpace(update.Author))

	if p.cfg.Filtering.MinLength > 0 && utf8.RuneCountInString(update.Description) < p.cfg.Filtering.MinLength {
		return true
	}

	for _, stopWord := range p.cfg.Filtering.StopWords {
		if stopWord == "" {
			continue
		}
		if strings.Contains(description, strings.ToLower(stopWord)) {
			return true
		}
	}

	for _, excludedAuthor := range p.cfg.Filtering.ExcludedAuthors {
		if author == strings.ToLower(strings.TrimSpace(excludedAuthor)) {
			return true
		}
	}

	return false
}

func (p *Processor) shouldSummarize(text string) bool {
	return p.cfg.Summarization.Threshold > 0 && utf8.RuneCountInString(text) > p.cfg.Summarization.Threshold
}

func (p *Processor) resolvePriority(text string) string {
	description := strings.ToLower(text)

	for _, keyword := range p.cfg.Prioritization.HighKeywords {
		if keyword == "" {
			continue
		}
		if strings.Contains(description, strings.ToLower(keyword)) {
			return priorityHigh
		}
	}

	for _, keyword := range p.cfg.Prioritization.LowKeywords {
		if keyword == "" {
			continue
		}
		if strings.Contains(description, strings.ToLower(keyword)) {
			return priorityLow
		}
	}

	return priorityMedium
}
