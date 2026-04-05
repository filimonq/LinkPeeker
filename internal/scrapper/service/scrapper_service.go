// Package service contains business logic for scrapper.
package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/chat"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
	"golang.org/x/sync/errgroup"
)

// ChatRepository defines storage operations for chats.
type ChatRepository interface {
	Add(ctx context.Context, id int64) error
	Remove(ctx context.Context, id int64) error
	Exists(ctx context.Context, id int64) (bool, error)
}

// LinkRepository defines storage operations for links.
type LinkRepository interface {
	GetOrCreate(ctx context.Context, url string) (*link.Link, error)
	FindOldest(ctx context.Context, limit int) ([]*link.Link, error)
	FindByURL(ctx context.Context, url string) (*link.Link, error)
	UpdateTime(ctx context.Context, id link.ID, updatedAt time.Time) error
	UpdateCheckedTime(ctx context.Context, id link.ID) error
}

// SubscriptionRepository defines storage operations for subscriptions.
type SubscriptionRepository interface {
	Add(ctx context.Context, chatID int64, linkID link.ID) error
	Remove(ctx context.Context, chatID int64, url string) (*link.Link, error)
	FindByChatID(ctx context.Context, chatID int64, limit, offset int) ([]*link.Link, error)
	FindChatsByLinkID(ctx context.Context, linkID link.ID) ([]int64, error)
	Exists(ctx context.Context, chatID int64, url string) (bool, error)
}

// TagRepository defines storage operations for tags.
type TagRepository interface {
	Add(ctx context.Context, chatID int64, linkID link.ID, tag string) error
	Remove(ctx context.Context, chatID int64, linkID link.ID, tag string) error
	FindBySubscription(ctx context.Context, chatID int64, linkID link.ID) ([]string, error)
}

// ScrapperService defines business operations for scrapper.
type ScrapperService interface {
	RegisterChat(ctx context.Context, chatID int64) error
	DeleteChat(ctx context.Context, chatID int64) error
	AddLink(ctx context.Context, chatID int64, url string, tags []string) (*link.WithTags, error)
	RemoveLink(ctx context.Context, chatID int64, url string) (*link.WithTags, error)
	ListLinks(ctx context.Context, chatID int64) ([]*link.WithTags, error)
	CheckUpdates(ctx context.Context) error
	AddTag(ctx context.Context, chatID int64, url string, tag string) error
	RemoveTag(ctx context.Context, chatID int64, url string, tag string) error
	ListTags(ctx context.Context, chatID int64, url string) ([]string, error)
}

// LinkChecker checks for updates at a given URL.
type LinkChecker interface {
	// GetUpdate returns full update info for the given URL.
	GetUpdate(ctx context.Context, url string) (*pkgdomain.UpdateInfo, error)
	// Supports returns true if the checker can handle this URL.
	Supports(url string) bool
}

// BotNotifier sends link updates to the bot.
type BotNotifier interface {
	SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error
}

type scrapperService struct {
	chats       ChatRepository
	links       LinkRepository
	subs        SubscriptionRepository
	tags        TagRepository
	checkers    []LinkChecker
	notifier    BotNotifier
	workerCount int
	batchSize   int
}

const defaultPageSize = 100
const defaultWorkerCount = 5

func NewScrapperService(
	chats ChatRepository,
	links LinkRepository,
	subs SubscriptionRepository,
	tags TagRepository,
	checkers []LinkChecker,
	notifier BotNotifier,
	workerCount int,
	batchSize int,
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

	if err = s.subs.Add(ctx, chatID, l.ID); err != nil {
		return nil, fmt.Errorf("add subscription: %w", err)
	}

	for _, tag := range tags {
		if err = s.tags.Add(ctx, chatID, l.ID, tag); err != nil {
			return nil, fmt.Errorf("add tag: %w", err)
		}
	}

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

	l, err := s.links.FindByURL(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("find link: %w", err)
	}

	fetchedTags, err := s.tags.FindBySubscription(ctx, chatID, l.ID)
	if err != nil {
		return nil, fmt.Errorf("fetch tags: %w", err)
	}

	removedLink, err := s.subs.Remove(ctx, chatID, url)
	if err != nil {
		return nil, fmt.Errorf("remove subscription: %w", err)
	}

	return &link.WithTags{Link: removedLink, Tags: fetchedTags}, nil
}

func (s *scrapperService) ListLinks(ctx context.Context, chatID int64) ([]*link.WithTags, error) {
	exists, err := s.chats.Exists(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("check chat: %w", err)
	}
	if !exists {
		return nil, chat.ErrNotFound
	}

	links, err := s.subs.FindByChatID(ctx, chatID, defaultPageSize, 0)
	if err != nil {
		return nil, fmt.Errorf("list links: %w", err)
	}

	result := make([]*link.WithTags, len(links))
	for i, l := range links {
		fetchedTags, tagsErr := s.tags.FindBySubscription(ctx, chatID, l.ID)
		if tagsErr != nil {
			return nil, fmt.Errorf("fetch tags for link %d: %w", l.ID, tagsErr)
		}
		result[i] = &link.WithTags{Link: l, Tags: fetchedTags}
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
				err = s.processSingleLink(gCtx, l)

				if updateErr := s.links.UpdateCheckedTime(gCtx, l.ID); updateErr != nil {
					slog.Error("failed to update checked time", "url", l.URL, "err", updateErr)
				}

				if err != nil {
					slog.Error("failed to process link", "url", l.URL, "err", err)
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

	if len(chatIDs) > 0 {
		update := pkgdomain.LinkUpdate{
			ID:          int64(l.ID),
			URL:         l.URL,
			Description: formatDescription(info),
			TgChatIDs:   chatIDs,
		}
		if err = s.notifier.SendUpdate(ctx, update); err != nil {
			return fmt.Errorf("send update: %w", err)
		}
	}

	if err = s.links.UpdateTime(ctx, l.ID, info.UpdatedAt); err != nil {
		return fmt.Errorf("update time: %w", err)
	}

	return nil
}

// formatDescription builds a human-readable update message from UpdateInfo.
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

		const maxLen = 200
		previewRunes := []rune(info.Preview)

		if len(previewRunes) > maxLen {
			sb.WriteString(string(previewRunes[:197]))
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
