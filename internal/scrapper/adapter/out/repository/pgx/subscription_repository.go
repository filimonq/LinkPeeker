package pgxrepo

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// SubscriptionRepository is a PostgreSQL implementation using raw SQL.
type SubscriptionRepository struct {
	db *pgxpool.Pool
}

// NewSubscriptionRepository creates a new pgx SubscriptionRepository.
func NewSubscriptionRepository(db *pgxpool.Pool) *SubscriptionRepository {
	return &SubscriptionRepository{db: db}
}

// Add adds a subscription in a transaction.
func (r *SubscriptionRepository) Add(ctx context.Context, chatID int64, linkID link.ID) error {
	exec := repository.GetExecutor(ctx, r.db)
	_, err := exec.Exec(ctx,
		`INSERT INTO subscriptions (chat_id, link_id) VALUES ($1, $2)`,
		chatID, linkID,
	)
	if err != nil {
		return fmt.Errorf("insert subscription: %w", err)
	}
	return nil
}

// Remove removes a subscription by chatID and url.
func (r *SubscriptionRepository) Remove(ctx context.Context, chatID int64, url string) (*link.Link, error) {
	exec := repository.GetExecutor(ctx, r.db)
	l := &link.Link{}
	err := exec.QueryRow(ctx,
		`DELETE FROM subscriptions s
		 USING links l
		 WHERE s.link_id = l.id AND s.chat_id = $1 AND l.url = $2
		 RETURNING l.id, l.url, l.updated_at`,
		chatID, url,
	).Scan(&l.ID, &l.URL, &l.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, link.ErrNotFound
		}
		return nil, fmt.Errorf("delete subscription: %w", err)
	}
	return l, nil
}

// FindByChatID returns all links for a chat with pagination.
func (r *SubscriptionRepository) FindByChatID(ctx context.Context, chatID int64, limit, offset int) ([]*link.Link, error) {
	exec := repository.GetExecutor(ctx, r.db)
	rows, err := exec.Query(ctx,
		`SELECT l.id, l.url, l.updated_at
		 FROM links l
		 JOIN subscriptions s ON s.link_id = l.id
		 WHERE s.chat_id = $1
		 LIMIT $2 OFFSET $3`,
		chatID, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("query subscriptions: %w", err)
	}
	defer rows.Close()

	var links []*link.Link
	for rows.Next() {
		l := &link.Link{}
		if err = rows.Scan(&l.ID, &l.URL, &l.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan link: %w", err)
		}
		links = append(links, l)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return links, nil
}

// FindChatsByLinkID returns all chat IDs subscribed to a link.
func (r *SubscriptionRepository) FindChatsByLinkID(ctx context.Context, linkID link.ID) ([]int64, error) {
	exec := repository.GetExecutor(ctx, r.db)
	rows, err := exec.Query(ctx,
		`SELECT chat_id FROM subscriptions WHERE link_id = $1`,
		linkID,
	)
	if err != nil {
		return nil, fmt.Errorf("query chats by link: %w", err)
	}
	defer rows.Close()

	var chatIDs []int64
	for rows.Next() {
		var chatID int64
		if err = rows.Scan(&chatID); err != nil {
			return nil, fmt.Errorf("scan chat id: %w", err)
		}
		chatIDs = append(chatIDs, chatID)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return chatIDs, nil
}

// Exists checks if a subscription exists.
func (r *SubscriptionRepository) Exists(ctx context.Context, chatID int64, url string) (bool, error) {
	exec := repository.GetExecutor(ctx, r.db)
	var exists bool
	err := exec.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM subscriptions s
			JOIN links l ON l.id = s.link_id
			WHERE s.chat_id = $1 AND l.url = $2
		)`,
		chatID, url,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check subscription exists: %w", err)
	}
	return exists, nil
}
