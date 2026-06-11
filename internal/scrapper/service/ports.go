package service

import (
	"context"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
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
	CountTrackedBySource(ctx context.Context) (map[string]int, error)
}

// TagRepository defines storage operations for tags.
type TagRepository interface {
	Add(ctx context.Context, chatID int64, linkID link.ID, tag string) error
	Remove(ctx context.Context, chatID int64, linkID link.ID, tag string) error
	FindBySubscription(ctx context.Context, chatID int64, linkID link.ID) ([]string, error)
	FindByChat(ctx context.Context, chatID int64) (map[link.ID][]string, error)
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

// Transactor runs a function inside a DB transaction.
type Transactor interface {
	WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}

// OutboxEntry represents a single record in the outbox table.
type OutboxEntry struct {
	ID      int64
	Payload []byte
}

// OutboxRepository persists outbox entries and tracks their delivery status.
type OutboxRepository interface {
	// Add inserts a new pending record. Must be called inside the same
	// transaction as the business operation it accompanies.
	Add(ctx context.Context, payload []byte) error

	// FetchPending returns up to limit undelivered entries, locked for update
	// so concurrent pollers do not pick the same row (SKIP LOCKED).
	FetchPending(ctx context.Context, limit int) ([]*OutboxEntry, error)

	// MarkSent flips the entry status to 'sent'. Called only after the
	// publisher has successfully delivered the message.
	MarkSent(ctx context.Context, id int64) error
}

// LinkCache caches list-links results keyed by chat ID.
type LinkCache interface {
	// Get returns the cached links for a chat, or (nil, nil) on cache miss.
	Get(ctx context.Context, chatID int64) ([]*link.WithTags, error)
	// Set stores the links for a chat.
	Set(ctx context.Context, chatID int64, links []*link.WithTags) error
	// Invalidate removes the cached entry for a chat.
	Invalidate(ctx context.Context, chatID int64) error
}
