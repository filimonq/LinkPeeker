// Package outbox provides the Transactional Outbox relay poller.
package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
)

// Publisher is the transport-level sender (Kafka producer).
type Publisher interface {
	SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error
}

// Repository is the persistence side of the outbox.
type Repository interface {
	FetchPending(ctx context.Context, limit int) ([]*service.OutboxEntry, error)
	MarkSent(ctx context.Context, id int64) error
}

// Poller drains the outbox table and forwards messages to the publisher.
type Poller struct {
	repo      Repository
	publisher Publisher
	interval  time.Duration
	batchSize int
	logger    *slog.Logger
}

func NewPoller(repo Repository, publisher Publisher, interval time.Duration, batchSize int, logger *slog.Logger) *Poller {
	return &Poller{repo: repo, publisher: publisher, interval: interval, batchSize: batchSize, logger: logger}
}

// Start runs the relay loop until ctx is cancelled.
func (p *Poller) Start(ctx context.Context) {
	p.logger.Info("outbox poller started", slog.Duration("interval", p.interval))
	p.poll(ctx)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("outbox poller stopped")
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *Poller) poll(ctx context.Context) {
	entries, err := p.repo.FetchPending(ctx, p.batchSize)
	if err != nil {
		p.logger.Error("outbox: fetch pending failed", slog.String("err", err.Error()))
		return
	}

	for _, entry := range entries {
		var update pkgdomain.LinkUpdate
		if err = json.Unmarshal(entry.Payload, &update); err != nil {
			p.logger.Error("outbox: bad payload", slog.Int64("id", entry.ID), slog.String("err", err.Error()))
			continue
		}

		if err = p.publisher.SendUpdate(ctx, update); err != nil {
			p.logger.Error("outbox: publish failed", slog.Int64("id", entry.ID), slog.String("err", err.Error()))
			continue
		}

		if err = p.repo.MarkSent(ctx, entry.ID); err != nil {
			p.logger.Error("outbox: mark sent failed", slog.Int64("id", entry.ID), slog.String("err", err.Error()))
		}
	}
}
