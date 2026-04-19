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

// GetOrCreate inserts a link if not exists and returns it.
func (r *LinkRepository) GetOrCreate(ctx context.Context, url string) (*link.Link, error) {
	exec := repository.GetExecutor(ctx, r.db)
	query, args, err := psql.Insert("links").
		Columns("url").
		Values(url).
		Suffix("ON CONFLICT (url) DO UPDATE SET url = EXCLUDED.url RETURNING id, url, updated_at").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	l := &link.Link{}
	if err = exec.QueryRow(ctx, query, args...).Scan(&l.ID, &l.URL, &l.UpdatedAt); err != nil {
		return nil, fmt.Errorf("get or create link: %w", err)
	}
	return l, nil
}

// FindOldest returns the oldest links based on the last_checked_at field.
func (r *LinkRepository) FindOldest(ctx context.Context, limit int) ([]*link.Link, error) {
	exec := repository.GetExecutor(ctx, r.db)
	query, args, err := psql.Select("l.id", "l.url", "l.updated_at").
		From("links l").
		Where("EXISTS (SELECT 1 FROM subscriptions s WHERE s.link_id = l.id)").
		OrderBy("l.last_checked_at ASC").
		Limit(uint64(limit)).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := exec.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query oldest links: %w", err)
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
	return links, nil
}

func (r *LinkRepository) UpdateCheckedTime(ctx context.Context, id link.ID) error {
	exec := repository.GetExecutor(ctx, r.db)
	query, args, err := psql.Update("links").
		Set("last_checked_at", sq.Expr("NOW()")).
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	if _, err = exec.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("update checked time: %w", err)
	}
	return nil
}

func (r *LinkRepository) UpdateTime(ctx context.Context, id link.ID, updatedAt time.Time) error {
	exec := repository.GetExecutor(ctx, r.db)
	query, args, err := psql.Update("links").
		Set("updated_at", updatedAt).
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	if _, err = exec.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("update link time: %w", err)
	}
	return nil
}

// FindByURL returns a link by url.
func (r *LinkRepository) FindByURL(ctx context.Context, url string) (*link.Link, error) {
	exec := repository.GetExecutor(ctx, r.db)
	query, args, err := psql.Select("id", "url", "updated_at").
		From("links").
		Where(sq.Eq{"url": url}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	l := &link.Link{}
	if err = exec.QueryRow(ctx, query, args...).Scan(&l.ID, &l.URL, &l.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, link.ErrNotFound
		}
		return nil, fmt.Errorf("find link by url: %w", err)
	}
	return l, nil
}
