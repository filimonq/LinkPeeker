package service

import (
	"context"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

// Publisher sends processed updates to the next Kafka topic.
type Publisher interface {
	SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error
}

// Summarizer shortens update descriptions when they exceed the threshold.
type Summarizer interface {
	Summarize(ctx context.Context, text string, threshold int) (string, error)
}
