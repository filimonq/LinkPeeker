package bot

import (
	"context"
	"fmt"
	"log/slog"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

type notifier interface {
	SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error
}

type FallbackNotifier struct {
	primary  notifier
	fallback notifier
	logger   *slog.Logger
}

func NewFallbackNotifier(primary, fallback notifier, logger *slog.Logger) *FallbackNotifier {
	return &FallbackNotifier{primary: primary, fallback: fallback, logger: logger}
}

func (f *FallbackNotifier) SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error {
	if err := f.primary.SendUpdate(ctx, update); err != nil {
		f.logger.Warn("primary HTTP notifier failed, using Kafka fallback",
			slog.String("error", err.Error()),
		)
		if fbErr := f.fallback.SendUpdate(ctx, update); fbErr != nil {
			return fmt.Errorf("fallback notifier failed: %w", fbErr)
		}
		return nil
	}
	return nil
}
