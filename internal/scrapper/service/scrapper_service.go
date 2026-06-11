// Package service contains business logic for scrapper.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/observability"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/chat"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
	"golang.org/x/sync/errgroup"
)

type scrapperService struct {
	chats       ChatRepository
	links       LinkRepository
	subs        SubscriptionRepository
	tags        TagRepository
	checkers    []LinkChecker
	notifier    BotNotifier
	outbox      OutboxRepository
	cache       LinkCache
	metrics     *observability.ScrapperMetrics
	workerCount int
	batchSize   int
	tx          Transactor
}

const defaultPageSize = 100
const defaultWorkerCount = 5
const descriptionPreviewMaxLen = 1000

func NewScrapperService(
	chats ChatRepository,
	links LinkRepository,
	subs SubscriptionRepository,
	tags TagRepository,
	checkers []LinkChecker,
	notifier BotNotifier,
	workerCount int,
	batchSize int,
	tx Transactor,
) ScrapperService {
	if workerCount <= 0 {
		workerCount = defaultWorkerCount
	}
	if batchSize <= 0 {
		batchSize = defaultPageSize
	}

	return &scrapperService{
		chats:       chats,
		links:       links,
		subs:        subs,
		tags:        tags,
		checkers:    checkers,
		notifier:    notifier,
		workerCount: workerCount,
		batchSize:   batchSize,
		tx:          tx,
	}
}

// WithOutbox enables the Transactional Outbox pattern. When set, updates are
// written to the outbox table (inside the same DB transaction as UpdateTime)
// instead of being sent directly via the notifier.
func WithOutbox(svc ScrapperService, outbox OutboxRepository) {
	if s, ok := svc.(*scrapperService); ok {
		s.outbox = outbox
	}
}

// WithCache enables Valkey caching for ListLinks.
func WithCache(svc ScrapperService, cache LinkCache) {
	if s, ok := svc.(*scrapperService); ok {
		s.cache = cache
	}
}

func (s *scrapperService) RegisterChat(ctx context.Context, chatID int64) error {
	if err := s.chats.Add(ctx, chatID); err != nil {
		return fmt.Errorf("register chat: %w", err)
	}
	return nil
}

func (s *scrapperService) DeleteChat(ctx context.Context, chatID int64) error {
	if err := s.chats.Remove(ctx, chatID); err != nil {
		return fmt.Errorf("delete chat: %w", err)
	}
	s.refreshTrackedLinksMetric(ctx)
	return nil
}

func (s *scrapperService) AddLink(ctx context.Context, chatID int64, url string, tags []string) (*link.WithTags, error) {
	exists, err := s.chats.Exists(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("check chat: %w", err)
	}
	if !exists {
		return nil, chat.ErrNotFound
	}

	l, err := s.links.GetOrCreate(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("get or create link: %w", err)
	}

	subExists, err := s.subs.Exists(ctx, chatID, url)
	if err != nil {
		return nil, fmt.Errorf("check subscription: %w", err)
	}
	if subExists {
		return nil, link.ErrAlreadyExists
	}

	if err = s.tx.WithinTransaction(ctx, func(txCtx context.Context) error {
		if err = s.subs.Add(txCtx, chatID, l.ID); err != nil {
			return fmt.Errorf("add subscription: %w", err)
		}
		for _, tag := range tags {
			if err = s.tags.Add(txCtx, chatID, l.ID, tag); err != nil {
				return fmt.Errorf("add tag: %w", err)
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction failed: %w", err)
	}

	s.invalidateCache(ctx, chatID)
	s.refreshTrackedLinksMetric(ctx)

	return &link.WithTags{Link: l, Tags: tags}, nil
}

func (s *scrapperService) RemoveLink(ctx context.Context, chatID int64, url string) (*link.WithTags, error) {
	exists, err := s.chats.Exists(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("check chat: %w", err)
	}
	if !exists {
		return nil, chat.ErrNotFound
	}

	var result *link.WithTags

	if err = s.tx.WithinTransaction(ctx, func(txCtx context.Context) error {
		l, tErr := s.links.FindByURL(txCtx, url)
		if tErr != nil {
			return fmt.Errorf("find link: %w", tErr)
		}

		fetchedTags, tErr := s.tags.FindBySubscription(txCtx, chatID, l.ID)
		if tErr != nil {
			return fmt.Errorf("fetch tags: %w", tErr)
		}

		removedLink, tErr := s.subs.Remove(txCtx, chatID, url)
		if tErr != nil {
			return fmt.Errorf("remove subscription: %w", tErr)
		}

		result = &link.WithTags{Link: removedLink, Tags: fetchedTags}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction failed: %w", err)
	}

	s.invalidateCache(ctx, chatID)
	s.refreshTrackedLinksMetric(ctx)

	return result, nil
}

func (s *scrapperService) ListLinks(ctx context.Context, chatID int64) ([]*link.WithTags, error) {
	exists, err := s.chats.Exists(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("check chat: %w", err)
	}
	if !exists {
		return nil, chat.ErrNotFound
	}

	if s.cache != nil {
		if cached, cErr := s.cache.Get(ctx, chatID); cErr == nil && cached != nil {
			return cached, nil
		}
	}

	links, err := s.subs.FindByChatID(ctx, chatID, defaultPageSize, 0)
	if err != nil {
		return nil, fmt.Errorf("list links: %w", err)
	}

	allTags, err := s.tags.FindByChat(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("fetch tags: %w", err)
	}

	result := make([]*link.WithTags, len(links))
	for i, l := range links {
		result[i] = &link.WithTags{Link: l, Tags: allTags[l.ID]}
	}

	if s.cache != nil {
		if cErr := s.cache.Set(ctx, chatID, result); cErr != nil {
			slog.Warn("cache set failed", slog.Int64("chat_id", chatID), slog.String("err", cErr.Error()))
		}
	}

	return result, nil
}

func (s *scrapperService) AddTag(ctx context.Context, chatID int64, url string, tag string) error {
	l, err := s.links.FindByURL(ctx, url)
	if err != nil {
		return fmt.Errorf("find link: %w", err)
	}
	if err = s.tags.Add(ctx, chatID, l.ID, tag); err != nil {
		return fmt.Errorf("add tag: %w", err)
	}
	return nil
}

func (s *scrapperService) RemoveTag(ctx context.Context, chatID int64, url string, tag string) error {
	l, err := s.links.FindByURL(ctx, url)
	if err != nil {
		return fmt.Errorf("find link: %w", err)
	}
	if err = s.tags.Remove(ctx, chatID, l.ID, tag); err != nil {
		return fmt.Errorf("remove tag: %w", err)
	}
	return nil
}

func (s *scrapperService) ListTags(ctx context.Context, chatID int64, url string) ([]string, error) {
	l, err := s.links.FindByURL(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("find link: %w", err)
	}
	tags, err := s.tags.FindBySubscription(ctx, chatID, l.ID)
	if err != nil {
		return nil, fmt.Errorf("list tags: %w", err)
	}
	return tags, nil
}

func (s *scrapperService) CheckUpdates(ctx context.Context) error {
	links, err := s.links.FindOldest(ctx, s.batchSize)
	if err != nil {
		return fmt.Errorf("find oldest links: %w", err)
	}
	if len(links) == 0 {
		return nil
	}

	g, gCtx := errgroup.WithContext(ctx)
	jobs := make(chan *link.Link)

	g.Go(func() error {
		defer close(jobs)
		for _, l := range links {
			select {
			case jobs <- l:
			case <-gCtx.Done():
				return gCtx.Err()
			}
		}
		return nil
	})

	var mu sync.Mutex
	var failedURLs []string

	for range s.workerCount {
		g.Go(func() error {
			for l := range jobs {
				processErr := s.processSingleLink(gCtx, l)

				if updateErr := s.links.UpdateCheckedTime(gCtx, l.ID); updateErr != nil {
					slog.Error("failed to update checked time", "url", l.URL, "err", updateErr)
				}

				if processErr != nil {
					slog.Error("failed to process link", "url", l.URL, "err", processErr)
					mu.Lock()
					failedURLs = append(failedURLs, l.URL)
					mu.Unlock()
				}
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return fmt.Errorf("worker pool error: %w", err)
	}

	if len(failedURLs) > 0 {
		slog.Warn("batch processing finished with errors",
			"failed_count", len(failedURLs),
			"failed_urls", failedURLs,
		)
	}

	return nil
}

func (s *scrapperService) processSingleLink(ctx context.Context, l *link.Link) error {
	checker := s.findChecker(l.URL)
	if checker == nil {
		return nil
	}

	info, err := checker.GetUpdate(ctx, l.URL)
	if err != nil {
		return fmt.Errorf("get update: %w", err)
	}

	if !info.UpdatedAt.After(l.UpdatedAt) {
		return nil
	}

	chatIDs, err := s.subs.FindChatsByLinkID(ctx, l.ID)
	if err != nil {
		return fmt.Errorf("find chats: %w", err)
	}

	if len(chatIDs) == 0 {
		return nil
	}

	update := pkgdomain.LinkUpdate{
		ID:          int64(l.ID),
		URL:         l.URL,
		Description: formatDescription(info),
		Author:      info.Author,
		TgChatIDs:   chatIDs,
	}

	if s.outbox != nil {
		// Transactional Outbox: write the event and update the link timestamp
		// atomically. The poller will pick it up and forward to Kafka.
		if txErr := s.tx.WithinTransaction(ctx, func(txCtx context.Context) error {
			payload, jsonErr := json.Marshal(update)
			if jsonErr != nil {
				return fmt.Errorf("marshal update for outbox: %w", jsonErr)
			}

			if addErr := s.outbox.Add(txCtx, payload); addErr != nil {
				return fmt.Errorf("write to outbox: %w", addErr)
			}

			if timeErr := s.links.UpdateTime(txCtx, l.ID, info.UpdatedAt); timeErr != nil {
				return fmt.Errorf("update link time: %w", timeErr)
			}

			return nil
		}); txErr != nil {
			return fmt.Errorf("outbox transaction: %w", txErr)
		}
		return nil
	}

	if err = s.notifier.SendUpdate(ctx, update); err != nil {
		return fmt.Errorf("send update: %w", err)
	}

	if err = s.links.UpdateTime(ctx, l.ID, info.UpdatedAt); err != nil {
		return fmt.Errorf("update time: %w", err)
	}

	return nil
}

// invalidateCache removes the cached entry for a chat (best-effort).
func (s *scrapperService) invalidateCache(ctx context.Context, chatID int64) {
	if s.cache == nil {
		return
	}
	if err := s.cache.Invalidate(ctx, chatID); err != nil {
		slog.Warn("cache invalidate failed", slog.Int64("chat_id", chatID), slog.String("err", err.Error()))
	}
}

func formatDescription(info *pkgdomain.UpdateInfo) string {
	if info.Title == "" && info.Author == "" {
		return "Обнаружены изменения"
	}

	var sb strings.Builder

	if info.Title != "" {
		sb.WriteString("📌 ")
		sb.WriteString(info.Title)
		sb.WriteString("\n")
	}

	if info.Author != "" {
		sb.WriteString("👤 ")
		sb.WriteString(info.Author)
		if !info.CreatedAt.IsZero() {
			sb.WriteString(" · ")
			sb.WriteString(info.CreatedAt.Format("02 Jan 15:04"))
		}
		sb.WriteString("\n")
	}

	if info.Preview != "" {
		sb.WriteString("\n")

		previewRunes := []rune(info.Preview)

		if len(previewRunes) > descriptionPreviewMaxLen {
			sb.WriteString(string(previewRunes[:descriptionPreviewMaxLen-3]))
			sb.WriteString("...")
		} else {
			sb.WriteString(info.Preview)
		}
	}

	return sb.String()
}

func (s *scrapperService) findChecker(url string) LinkChecker {
	for _, c := range s.checkers {
		if c.Supports(url) {
			return c
		}
	}
	return nil
}
