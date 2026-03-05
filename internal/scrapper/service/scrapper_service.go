// Package service contains business logic for scrapper.
package service

import (
	"context"
	"fmt"

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
}

// ScrapperService defines business operations for scrapper.
type ScrapperService interface {
	RegisterChat(ctx context.Context, chatID int64) error
	DeleteChat(ctx context.Context, chatID int64) error
	AddLink(ctx context.Context, chatID int64, url string, tags []string, filters []string) (*link.Link, error)
	RemoveLink(ctx context.Context, chatID int64, url string) (*link.Link, error)
	ListLinks(ctx context.Context, chatID int64) ([]*link.Link, error)
}

type scrapperService struct {
	chats ChatRepository
	links LinkRepository
}

// NewScrapperService creates a new ScrapperService.
func NewScrapperService(chats ChatRepository, links LinkRepository) ScrapperService {
	return &scrapperService{chats: chats, links: links}
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

func (s *scrapperService) AddLink(ctx context.Context, chatID int64, url string, tags []string, filters []string) (*link.Link, error) {
	exists, err := s.chats.Exists(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("check chat: %w", err)
	}
	if !exists {
		return nil, chat.ErrNotFound
	}

	l := &link.Link{
		URL:     url,
		ChatID:  chatID,
		Tags:    tags,
		Filters: filters,
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
