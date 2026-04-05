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

// Add adds a tag to a link.
func (r *TagRepository) Add(ctx context.Context, linkID link.ID, tag string) error {
	_, err := r.db.Exec(ctx,
		"INSERT INTO link_tags (link_id, tag) VALUES ($1, $2) ON CONFLICT DO NOTHING",
		linkID, tag,
	)
	if err != nil {
		return fmt.Errorf("insert tag: %w", err)
	}
	return nil
}

// Remove removes a tag from a link.
func (r *TagRepository) Remove(ctx context.Context, linkID link.ID, tag string) error {
	_, err := r.db.Exec(ctx,
		"DELETE FROM link_tags WHERE link_id = $1 AND tag = $2",
		linkID, tag,
	)
	if err != nil {
		return fmt.Errorf("delete tag: %w", err)
	}
	return nil
}

// FindByLinkID returns all tags for a link.
func (r *TagRepository) FindByLinkID(ctx context.Context, linkID link.ID) ([]string, error) {
	rows, err := r.db.Query(ctx,
		"SELECT tag FROM link_tags WHERE link_id = $1",
		linkID,
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
