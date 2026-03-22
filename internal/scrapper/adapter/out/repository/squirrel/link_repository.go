package squirrel

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// LinkRepository is a PostgreSQL implementation using squirrel.
type LinkRepository struct {
	db *pgxpool.Pool
}

// NewLinkRepository creates a new squirrel LinkRepository.
func NewLinkRepository(db *pgxpool.Pool) *LinkRepository {
	return &LinkRepository{db: db}
}

// Add inserts a link and its tags in a transaction.
func (r *LinkRepository) Add(ctx context.Context, l *link.Link) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query, args, err := psql.Insert("links").
		Columns("url", "chat_id").
		Values(l.URL, l.ChatID).
		Suffix("RETURNING id, created_at, updated_at").
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	if err = tx.QueryRow(ctx, query, args...).Scan(&l.ID, &l.CreatedAt, &l.UpdatedAt); err != nil {
		if repository.IsUniqueViolation(err) {
			return link.ErrAlreadyExists
		}
		return fmt.Errorf("insert link: %w", err)
	}

	for _, tag := range l.Tags {
		tagQuery, tagArgs, tagErr := psql.Insert("link_tags").
			Columns("link_id", "tag").
			Values(l.ID, tag).
			ToSql()
		if tagErr != nil {
			return fmt.Errorf("build tag query: %w", tagErr)
		}
		if _, tagErr = tx.Exec(ctx, tagQuery, tagArgs...); tagErr != nil {
			return fmt.Errorf("insert tag: %w", tagErr)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// Remove deletes a link by chatID and url.
func (r *LinkRepository) Remove(ctx context.Context, chatID int64, url string) (*link.Link, error) {
	query, args, err := psql.Delete("links").
		Where(sq.Eq{"chat_id": chatID, "url": url}).
		Suffix("RETURNING id, url, chat_id, created_at, updated_at").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	l := &link.Link{}
	if err = r.db.QueryRow(ctx, query, args...).
		Scan(&l.ID, &l.URL, &l.ChatID, &l.CreatedAt, &l.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, link.ErrNotFound
		}
		return nil, fmt.Errorf("delete link: %w", err)
	}
	return l, nil
}

// FindByChatID returns all links for a chat with their tags.
func (r *LinkRepository) FindByChatID(ctx context.Context, chatID int64) ([]*link.Link, error) {
	query, args, err := psql.Select(
		"l.id", "l.url", "l.chat_id", "l.created_at", "l.updated_at",
		"COALESCE(array_agg(lt.tag) FILTER (WHERE lt.tag IS NOT NULL), '{}')",
	).
		From("links l").
		LeftJoin("link_tags lt ON lt.link_id = l.id").
		Where(sq.Eq{"l.chat_id": chatID}).
		GroupBy("l.id").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query links: %w", err)
	}
	defer rows.Close()

	links, err := repository.ScanLinks(rows)
	if err != nil {
		return nil, fmt.Errorf("scan links: %w", err)
	}
	return links, nil
}

// FindAll returns all links with their tags.
func (r *LinkRepository) FindAll(ctx context.Context) ([]*link.Link, error) {
	query, args, err := psql.Select(
		"l.id", "l.url", "l.chat_id", "l.created_at", "l.updated_at",
		"COALESCE(array_agg(lt.tag) FILTER (WHERE lt.tag IS NOT NULL), '{}')",
	).
		From("links l").
		LeftJoin("link_tags lt ON lt.link_id = l.id").
		GroupBy("l.id").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query all links: %w", err)
	}
	defer rows.Close()

	links, err := repository.ScanLinks(rows)
	if err != nil {
		return nil, fmt.Errorf("scan links: %w", err)
	}
	return links, nil
}

// Exists checks if a link exists for a given chat and url.
func (r *LinkRepository) Exists(ctx context.Context, chatID int64, url string) (bool, error) {
	inner, args, err := psql.Select("1").
		From("links").
		Where(sq.Eq{"chat_id": chatID, "url": url}).
		ToSql()
	if err != nil {
		return false, fmt.Errorf("build query: %w", err)
	}

	var exists bool
	if err = r.db.QueryRow(ctx, "SELECT EXISTS("+inner+")", args...).Scan(&exists); err != nil {
		return false, fmt.Errorf("check link exists: %w", err)
	}
	return exists, nil
}

// UpdateTime updates the updated_at field of a link.
func (r *LinkRepository) UpdateTime(ctx context.Context, id link.ID, updatedAt time.Time) error {
	query, args, err := psql.Update("links").
		Set("updated_at", updatedAt).
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	tag, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update link time: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return link.ErrNotFound
	}
	return nil
}
