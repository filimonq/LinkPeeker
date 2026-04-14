package squirrel

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// TagRepository is a PostgreSQL implementation using squirrel.
type TagRepository struct {
	db *pgxpool.Pool
}

// NewTagRepository creates a new squirrel TagRepository.
func NewTagRepository(db *pgxpool.Pool) *TagRepository {
	return &TagRepository{db: db}
}

// Add adds a tag to a subscription.
func (r *TagRepository) Add(ctx context.Context, chatID int64, linkID link.ID, tag string) error {
	exec := repository.GetExecutor(ctx, r.db)
	query, args, err := psql.Insert("link_tags").
		Columns("chat_id", "link_id", "tag").
		Values(chatID, linkID, tag).
		Suffix("ON CONFLICT DO NOTHING").
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}
	if _, err = exec.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("insert tag: %w", err)
	}
	return nil
}

// Remove removes a tag from a subscription.
func (r *TagRepository) Remove(ctx context.Context, chatID int64, linkID link.ID, tag string) error {
	exec := repository.GetExecutor(ctx, r.db)
	query, args, err := psql.Delete("link_tags").
		Where(sq.Eq{"chat_id": chatID, "link_id": linkID, "tag": tag}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}
	if _, err = exec.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("delete tag: %w", err)
	}
	return nil
}

// FindBySubscription returns all tags for a subscription.
func (r *TagRepository) FindBySubscription(ctx context.Context, chatID int64, linkID link.ID) ([]string, error) {
	exec := repository.GetExecutor(ctx, r.db)
	query, args, err := psql.Select("tag").
		From("link_tags").
		Where(sq.Eq{"chat_id": chatID, "link_id": linkID}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := exec.Query(ctx, query, args...)
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
	exec := repository.GetExecutor(ctx, r.db)
	query, args, err := psql.Select("link_id", "tag").
		From("link_tags").
		Where(sq.Eq{"chat_id": chatID}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := exec.Query(ctx, query, args...)
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
