package service

import (
	"context"
	"fmt"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/observability"
)

const (
	botScopeCommand          = "command"
	botScopeScrapperSyncAPI  = "scrapper_sync_api"
	botScopeScrapperAsyncAPI = "scrapper_async_api"
)

type instrumentedScrapperClient struct {
	next    ScrapperClient
	metrics *observability.BotMetrics
}

type instrumentedUpdateService struct {
	next    UpdateService
	metrics *observability.BotMetrics
}

func InstrumentScrapperClient(next ScrapperClient, metrics *observability.BotMetrics) ScrapperClient {
	return &instrumentedScrapperClient{next: next, metrics: metrics}
}

func InstrumentUpdateService(next UpdateService, metrics *observability.BotMetrics) UpdateService {
	return &instrumentedUpdateService{next: next, metrics: metrics}
}

func ObserveCommand(metrics *observability.BotMetrics, command domain.CommandType, startedAt time.Time) {
	metrics.ObserveCommandRequest(string(command))
	metrics.ObserveCommandDuration(botScopeCommand, string(command), time.Since(startedAt))
}

func (c *instrumentedScrapperClient) RegisterChat(ctx context.Context, chatID int64) error {
	return c.observeErr("RegisterChat", func() error {
		return c.next.RegisterChat(ctx, chatID)
	})
}

func (c *instrumentedScrapperClient) DeleteChat(ctx context.Context, chatID int64) error {
	return c.observeErr("DeleteChat", func() error {
		return c.next.DeleteChat(ctx, chatID)
	})
}

func (c *instrumentedScrapperClient) AddLink(ctx context.Context, chatID int64, url string, tags []string) (*LinkInfo, error) {
	start := time.Now()
	result, err := c.next.AddLink(ctx, chatID, url, tags)
	c.metrics.ObserveCommandDuration(botScopeScrapperSyncAPI, "AddLink", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe add link: %w", err)
	}
	return result, nil
}

func (c *instrumentedScrapperClient) RemoveLink(ctx context.Context, chatID int64, url string) (*LinkInfo, error) {
	start := time.Now()
	result, err := c.next.RemoveLink(ctx, chatID, url)
	c.metrics.ObserveCommandDuration(botScopeScrapperSyncAPI, "RemoveLink", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe remove link: %w", err)
	}
	return result, nil
}

func (c *instrumentedScrapperClient) ListLinks(ctx context.Context, chatID int64) ([]*LinkInfo, error) {
	start := time.Now()
	result, err := c.next.ListLinks(ctx, chatID)
	c.metrics.ObserveCommandDuration(botScopeScrapperSyncAPI, "ListLinks", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe list links: %w", err)
	}
	return result, nil
}

func (c *instrumentedScrapperClient) observeErr(scopeType string, fn func() error) error {
	start := time.Now()
	err := fn()
	c.metrics.ObserveCommandDuration(botScopeScrapperSyncAPI, scopeType, time.Since(start))
	if err != nil {
		return fmt.Errorf("observe %s: %w", scopeType, err)
	}
	return err
}

func (s *instrumentedUpdateService) SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error {
	start := time.Now()
	if err := s.next.SendUpdate(ctx, update); err != nil {
		return fmt.Errorf("observe send update: %w", err)
	}
	s.metrics.ObserveCommandDuration(botScopeScrapperAsyncAPI, "SendUpdate", time.Since(start))

	for range update.TgChatIDs {
		s.metrics.IncSentNotification()
	}
	return nil
}
