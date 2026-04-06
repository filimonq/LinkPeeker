package pgxrepo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// TagRepository is a PostgreSQL implementation using raw SQL.
type TagRepository struct {
	db *pgxpool.Pool
}

// NewTagRepository creates a new pgx TagRepository.
func NewTagRepository(db *pgxpool.Pool) *TagRepository {
	return &TagRepository{db: db}
}

// Add adds a tag to a subscription.
func (r *TagRepository) Add(ctx context.Context, chatID int64, linkID link.ID, tag string) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO link_tags (chat_id, link_id, tag) VALUES ($1, $2, $3)
		 ON CONFLICT DO NOTHING`,
		chatID, linkID, tag,
	)
	if err != nil {
		return fmt.Errorf("insert tag: %w", err)
	}
	return nil
}

// Remove removes a tag from a subscription.
func (r *TagRepository) Remove(ctx context.Context, chatID int64, linkID link.ID, tag string) error {
	_, err := r.db.Exec(ctx,
		`DELETE FROM link_tags WHERE chat_id = $1 AND link_id = $2 AND tag = $3`,
		chatID, linkID, tag,
	)
	if err != nil {
		return fmt.Errorf("delete tag: %w", err)
	}
	return nil
}

// FindBySubscription returns all tags for a subscription.
func (r *TagRepository) FindBySubscription(ctx context.Context, chatID int64, linkID link.ID) ([]string, error) {
	rows, err := r.db.Query(ctx,
		`SELECT tag FROM link_tags WHERE chat_id = $1 AND link_id = $2`,
		chatID, linkID,
	)
	if err != nil {
		return nil, fmt.Errorf("query tags: %w", err)
	}
	defer rows.Close()

	var tags []string
	for rows.Next() {
		var tag string
		if err = rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("scan tag: %w", err)
		}
		tags = append(tags, tag)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return tags, nil
}

// FindByChat returns all tags for all subscriptions of a chat grouped by link ID.
func (r *TagRepository) FindByChat(ctx context.Context, chatID int64) (map[link.ID][]string, error) {
	rows, err := r.db.Query(ctx,
		`SELECT link_id, tag FROM link_tags WHERE chat_id = $1`,
		chatID,
	)
	if err != nil {
		return nil, fmt.Errorf("query tags: %w", err)
	}
	defer rows.Close()

	result := make(map[link.ID][]string)
	for rows.Next() {
		var linkID link.ID
		var tag string
		if err = rows.Scan(&linkID, &tag); err != nil {
			return nil, fmt.Errorf("scan tag: %w", err)
		}
		result[linkID] = append(result[linkID], tag)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}
