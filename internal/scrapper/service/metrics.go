package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/observability"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

const (
	scrapperScopeDatabase       = "database"
	scrapperScopeExternalSource = "external_source"
	scrapperScopeLLMAgent       = "llm_agent"
	scrapperScopeKafka          = "kafka"
)

func WithMetrics(svc ScrapperService, metrics *observability.ScrapperMetrics) {
	if s, ok := svc.(*scrapperService); ok {
		s.metrics = metrics
		s.refreshTrackedLinksMetric(context.Background())
	}
}

type instrumentedChatRepository struct {
	next    ChatRepository
	metrics *observability.ScrapperMetrics
}

type instrumentedLinkRepository struct {
	next    LinkRepository
	metrics *observability.ScrapperMetrics
}

type instrumentedSubscriptionRepository struct {
	next    SubscriptionRepository
	metrics *observability.ScrapperMetrics
}

type instrumentedTagRepository struct {
	next    TagRepository
	metrics *observability.ScrapperMetrics
}

type instrumentedOutboxRepository struct {
	next    OutboxRepository
	metrics *observability.ScrapperMetrics
}

type instrumentedTransactor struct {
	next    Transactor
	metrics *observability.ScrapperMetrics
}

type instrumentedLinkChecker struct {
	next      LinkChecker
	metrics   *observability.ScrapperMetrics
	scopeType string
}

type instrumentedBotNotifier struct {
	next      BotNotifier
	metrics   *observability.ScrapperMetrics
	scopeType string
}

func InstrumentChatRepository(next ChatRepository, metrics *observability.ScrapperMetrics) ChatRepository {
	return &instrumentedChatRepository{next: next, metrics: metrics}
}

func InstrumentLinkRepository(next LinkRepository, metrics *observability.ScrapperMetrics) LinkRepository {
	return &instrumentedLinkRepository{next: next, metrics: metrics}
}

func InstrumentSubscriptionRepository(next SubscriptionRepository, metrics *observability.ScrapperMetrics) SubscriptionRepository {
	return &instrumentedSubscriptionRepository{next: next, metrics: metrics}
}

func InstrumentTagRepository(next TagRepository, metrics *observability.ScrapperMetrics) TagRepository {
	return &instrumentedTagRepository{next: next, metrics: metrics}
}

func InstrumentOutboxRepository(next OutboxRepository, metrics *observability.ScrapperMetrics) OutboxRepository {
	return &instrumentedOutboxRepository{next: next, metrics: metrics}
}

func InstrumentTransactor(next Transactor, metrics *observability.ScrapperMetrics) Transactor {
	return &instrumentedTransactor{next: next, metrics: metrics}
}

func InstrumentLinkChecker(next LinkChecker, metrics *observability.ScrapperMetrics) LinkChecker {
	return &instrumentedLinkChecker{
		next:      next,
		metrics:   metrics,
		scopeType: checkerScopeType(next),
	}
}

func InstrumentBotNotifier(next BotNotifier, metrics *observability.ScrapperMetrics, scopeType string) BotNotifier {
	return &instrumentedBotNotifier{
		next:      next,
		metrics:   metrics,
		scopeType: scopeType,
	}
}

func (r *instrumentedChatRepository) Add(ctx context.Context, id int64) error {
	return observeErr(r.metrics, scrapperScopeDatabase, "chats", func() error {
		return r.next.Add(ctx, id)
	})
}

func (r *instrumentedChatRepository) Remove(ctx context.Context, id int64) error {
	return observeErr(r.metrics, scrapperScopeDatabase, "chats", func() error {
		return r.next.Remove(ctx, id)
	})
}

func (r *instrumentedChatRepository) Exists(ctx context.Context, id int64) (bool, error) {
	start := time.Now()
	result, err := r.next.Exists(ctx, id)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "chats", time.Since(start))
	if err != nil {
		return false, fmt.Errorf("observe chat exists: %w", err)
	}
	return result, nil
}

func (r *instrumentedLinkRepository) GetOrCreate(ctx context.Context, url string) (*link.Link, error) {
	start := time.Now()
	result, err := r.next.GetOrCreate(ctx, url)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "links", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe get or create link: %w", err)
	}
	return result, nil
}

func (r *instrumentedLinkRepository) FindOldest(ctx context.Context, limit int) ([]*link.Link, error) {
	start := time.Now()
	result, err := r.next.FindOldest(ctx, limit)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "links", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe find oldest links: %w", err)
	}
	return result, nil
}

func (r *instrumentedLinkRepository) FindByURL(ctx context.Context, url string) (*link.Link, error) {
	start := time.Now()
	result, err := r.next.FindByURL(ctx, url)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "links", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe find link by url: %w", err)
	}
	return result, nil
}

func (r *instrumentedLinkRepository) UpdateTime(ctx context.Context, id link.ID, updatedAt time.Time) error {
	return observeErr(r.metrics, scrapperScopeDatabase, "links", func() error {
		return r.next.UpdateTime(ctx, id, updatedAt)
	})
}

func (r *instrumentedLinkRepository) UpdateCheckedTime(ctx context.Context, id link.ID) error {
	return observeErr(r.metrics, scrapperScopeDatabase, "links", func() error {
		return r.next.UpdateCheckedTime(ctx, id)
	})
}

func (r *instrumentedSubscriptionRepository) Add(ctx context.Context, chatID int64, linkID link.ID) error {
	return observeErr(r.metrics, scrapperScopeDatabase, "subscriptions", func() error {
		return r.next.Add(ctx, chatID, linkID)
	})
}

func (r *instrumentedSubscriptionRepository) Remove(ctx context.Context, chatID int64, url string) (*link.Link, error) {
	start := time.Now()
	result, err := r.next.Remove(ctx, chatID, url)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "subscriptions", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe remove subscription: %w", err)
	}
	return result, nil
}

func (r *instrumentedSubscriptionRepository) FindByChatID(ctx context.Context, chatID int64, limit, offset int) ([]*link.Link, error) {
	start := time.Now()
	result, err := r.next.FindByChatID(ctx, chatID, limit, offset)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "subscriptions", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe find subscriptions by chat: %w", err)
	}
	return result, nil
}

func (r *instrumentedSubscriptionRepository) FindChatsByLinkID(ctx context.Context, linkID link.ID) ([]int64, error) {
	start := time.Now()
	result, err := r.next.FindChatsByLinkID(ctx, linkID)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "subscriptions", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe find chats by link: %w", err)
	}
	return result, nil
}

func (r *instrumentedSubscriptionRepository) Exists(ctx context.Context, chatID int64, url string) (bool, error) {
	start := time.Now()
	result, err := r.next.Exists(ctx, chatID, url)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "subscriptions", time.Since(start))
	if err != nil {
		return false, fmt.Errorf("observe subscription exists: %w", err)
	}
	return result, nil
}

func (r *instrumentedSubscriptionRepository) CountTrackedBySource(ctx context.Context) (map[string]int, error) {
	start := time.Now()
	result, err := r.next.CountTrackedBySource(ctx)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "subscriptions", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe count tracked by source: %w", err)
	}
	return result, nil
}

func (r *instrumentedTagRepository) Add(ctx context.Context, chatID int64, linkID link.ID, tag string) error {
	return observeErr(r.metrics, scrapperScopeDatabase, "tags", func() error {
		return r.next.Add(ctx, chatID, linkID, tag)
	})
}

func (r *instrumentedTagRepository) Remove(ctx context.Context, chatID int64, linkID link.ID, tag string) error {
	return observeErr(r.metrics, scrapperScopeDatabase, "tags", func() error {
		return r.next.Remove(ctx, chatID, linkID, tag)
	})
}

func (r *instrumentedTagRepository) FindBySubscription(ctx context.Context, chatID int64, linkID link.ID) ([]string, error) {
	start := time.Now()
	result, err := r.next.FindBySubscription(ctx, chatID, linkID)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "tags", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe find tags by subscription: %w", err)
	}
	return result, nil
}

func (r *instrumentedTagRepository) FindByChat(ctx context.Context, chatID int64) (map[link.ID][]string, error) {
	start := time.Now()
	result, err := r.next.FindByChat(ctx, chatID)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "tags", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe find tags by chat: %w", err)
	}
	return result, nil
}

func (r *instrumentedOutboxRepository) Add(ctx context.Context, payload []byte) error {
	return observeErr(r.metrics, scrapperScopeDatabase, "outbox", func() error {
		return r.next.Add(ctx, payload)
	})
}

func (r *instrumentedOutboxRepository) FetchPending(ctx context.Context, limit int) ([]*OutboxEntry, error) {
	start := time.Now()
	result, err := r.next.FetchPending(ctx, limit)
	r.metrics.ObserveRequestDuration(scrapperScopeDatabase, "outbox", time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe fetch pending outbox: %w", err)
	}
	return result, nil
}

func (r *instrumentedOutboxRepository) MarkSent(ctx context.Context, id int64) error {
	return observeErr(r.metrics, scrapperScopeDatabase, "outbox", func() error {
		return r.next.MarkSent(ctx, id)
	})
}

func (t *instrumentedTransactor) WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return observeErr(t.metrics, scrapperScopeDatabase, "transaction", func() error {
		return t.next.WithinTransaction(ctx, fn)
	})
}

func (c *instrumentedLinkChecker) GetUpdate(ctx context.Context, url string) (*pkgdomain.UpdateInfo, error) {
	start := time.Now()
	result, err := c.next.GetUpdate(ctx, url)
	c.metrics.ObserveRequestDuration(scrapperScopeExternalSource, c.scopeType, time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("observe checker get update: %w", err)
	}
	return result, nil
}

func (c *instrumentedLinkChecker) Supports(url string) bool {
	return c.next.Supports(url)
}

func (n *instrumentedBotNotifier) SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error {
	return observeErr(n.metrics, scrapperScopeKafka, n.scopeType, func() error {
		return n.next.SendUpdate(ctx, update)
	})
}

func observeErr(metrics *observability.ScrapperMetrics, scope, scopeType string, fn func() error) error {
	start := time.Now()
	err := fn()
	metrics.ObserveRequestDuration(scope, scopeType, time.Since(start))
	if err != nil {
		return fmt.Errorf("observe %s/%s: %w", scope, scopeType, err)
	}
	return err
}

func checkerScopeType(checker LinkChecker) string {
	switch {
	case checker.Supports("https://github.com/openai/openai-go"):
		return "github.com"
	case checker.Supports("https://stackoverflow.com/questions/1/example"):
		return "stackoverflow.com"
	default:
		return "unknown"
	}
}

func normalizeTrackedSource(rawURL string) string {
	switch {
	case strings.HasPrefix(rawURL, "https://github.com"):
		return "github"
	case strings.HasPrefix(rawURL, "https://stackoverflow.com"):
		return "stackoverflow"
	default:
		return "other"
	}
}

// NormalizeTrackedSourceForMetrics maps URLs to dashboard-friendly source labels.
func NormalizeTrackedSourceForMetrics(rawURL string) string {
	return normalizeTrackedSource(rawURL)
}

func (s *scrapperService) refreshTrackedLinksMetric(ctx context.Context) {
	if s.metrics == nil {
		return
	}

	counts, err := s.subs.CountTrackedBySource(ctx)
	if err != nil {
		slog.Error("refresh tracked links metrics", "err", err)
		return
	}

	s.metrics.SetTrackedLinks("github", 0)
	s.metrics.SetTrackedLinks("stackoverflow", 0)
	s.metrics.SetTrackedLinks("other", 0)

	for source, count := range counts {
		s.metrics.SetTrackedLinks(source, float64(count))
	}
}
