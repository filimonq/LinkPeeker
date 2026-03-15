// Package service contains business logic for scrapper.
package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/chat"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// ChatRepository defines storage operations for chats.
type ChatRepository interface {
	Add(ctx context.Context, id int64) error
	Remove(ctx context.Context, id int64) error
	Exists(ctx context.Context, id int64) (bool, error)
}

// LinkRepository defines storage operations for links.
type LinkRepository interface {
	Add(ctx context.Context, l *link.Link) error
	Remove(ctx context.Context, chatID int64, url string) (*link.Link, error)
	FindByChatID(ctx context.Context, chatID int64) ([]*link.Link, error)
	FindAll(ctx context.Context) ([]*link.Link, error)
	Exists(ctx context.Context, chatID int64, url string) (bool, error)
	UpdateTime(ctx context.Context, id link.ID, updatedAt time.Time) error
}

// ScrapperService defines business operations for scrapper.
type ScrapperService interface {
	RegisterChat(ctx context.Context, chatID int64) error
	DeleteChat(ctx context.Context, chatID int64) error
	AddLink(ctx context.Context, chatID int64, url string, tags []string) (*link.Link, error)
	RemoveLink(ctx context.Context, chatID int64, url string) (*link.Link, error)
	ListLinks(ctx context.Context, chatID int64) ([]*link.Link, error)
	CheckUpdates(ctx context.Context) error
}

// LinkChecker checks for updates at a given URL.
type LinkChecker interface {
	GetLastUpdated(ctx context.Context, url string) (time.Time, error)
	Supports(url string) bool
}

// BotNotifier sends link updates to the bot.
type BotNotifier interface {
	SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error
}

type scrapperService struct {
	chats    ChatRepository
	links    LinkRepository
	checkers []LinkChecker
	notifier BotNotifier
}

// NewScrapperService creates a new ScrapperService.
func NewScrapperService(chats ChatRepository, links LinkRepository, checkers []LinkChecker, notifier BotNotifier) ScrapperService {
	return &scrapperService{
		chats:    chats,
		links:    links,
		checkers: checkers,
		notifier: notifier,
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

func (s *scrapperService) AddLink(ctx context.Context, chatID int64, url string, tags []string) (*link.Link, error) {
	exists, err := s.chats.Exists(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("check chat: %w", err)
	}
	if !exists {
		return nil, chat.ErrNotFound
	}

	l := &link.Link{
		URL:    url,
		ChatID: chatID,
		Tags:   tags,
	}

	if err := s.links.Add(ctx, l); err != nil {
		return nil, fmt.Errorf("add link: %w", err)
	}
	return l, nil
}

func (s *scrapperService) RemoveLink(ctx context.Context, chatID int64, url string) (*link.Link, error) {
	exists, err := s.chats.Exists(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("check chat: %w", err)
	}
	if !exists {
		return nil, chat.ErrNotFound
	}

	l, err := s.links.Remove(ctx, chatID, url)
	if err != nil {
		return nil, fmt.Errorf("remove link: %w", err)
	}
	return l, nil
}

func (s *scrapperService) ListLinks(ctx context.Context, chatID int64) ([]*link.Link, error) {
	exists, err := s.chats.Exists(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("check chat: %w", err)
	}
	if !exists {
		return nil, chat.ErrNotFound
	}

	links, err := s.links.FindByChatID(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("list links: %w", err)
	}
	return links, nil
}

func (s *scrapperService) CheckUpdates(ctx context.Context) error {
	links, err := s.links.FindAll(ctx)
	if err != nil {
		return fmt.Errorf("find all links: %w", err)
	}

	for _, l := range links {
		checker := s.findChecker(l.URL)
		if checker == nil {
			continue
		}

		updatedAt, err := checker.GetLastUpdated(ctx, l.URL)
		if err != nil {
			continue
		}

		if updatedAt.After(l.UpdatedAt) {
			if err := s.notifier.SendUpdate(ctx, pkgdomain.LinkUpdate{
				ID:        int64(l.ID),
				URL:       l.URL,
				TgChatIDs: []int64{l.ChatID},
			}); err != nil {
				slog.Error("send update", "url", l.URL, "err", err)
				continue
			}

			if err := s.links.UpdateTime(ctx, l.ID, updatedAt); err != nil {
				slog.Error("update time", "url", l.URL, "err", err)
			}
		}
	}
	return nil
}

func (s *scrapperService) findChecker(url string) LinkChecker {
	for _, c := range s.checkers {
		if c.Supports(url) {
			return c
		}
	}
	return nil
}
