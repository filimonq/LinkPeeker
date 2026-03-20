// Package scheduler provides periodic task scheduling.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-co-op/gocron/v2"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
)

// Scheduler periodically checks for link updates.
type Scheduler struct {
	scheduler gocron.Scheduler
	svc       service.ScrapperService
	logger    *slog.Logger
	interval  time.Duration
}

// NewScheduler creates a new Scheduler.
func NewScheduler(svc service.ScrapperService, logger *slog.Logger, interval time.Duration) (*Scheduler, error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}

	return &Scheduler{
		scheduler: s,
		svc:       svc,
		logger:    logger,
		interval:  interval,
	}, nil
}

// Start starts the scheduler.
func (s *Scheduler) Start() error {
	_, err := s.scheduler.NewJob(
		gocron.DurationJob(s.interval),
		gocron.NewTask(func() {
			ctx := context.Background()
			if err := s.svc.CheckUpdates(ctx); err != nil {
				s.logger.Error("check updates failed", slog.String("error", err.Error()))
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}

	s.scheduler.Start()
	return nil
}

// Stop stops the scheduler.
func (s *Scheduler) Stop() error {
	if err := s.scheduler.Shutdown(); err != nil {
		return fmt.Errorf("scheduler shutdown: %w", err)
	}
	return nil
}
