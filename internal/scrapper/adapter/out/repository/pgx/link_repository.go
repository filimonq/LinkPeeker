package pgxrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// LinkRepository is a PostgreSQL implementation 	using raw SQL.
type LinkRepository struct {
	db *pgxpool.Pool
}

// NewLinkRepository creates a new pgx LinkRepository.
func NewLinkRepository(db *pgxpool.Pool) *LinkRepository {
	return &LinkRepository{db: db}
}

// GetOrCreate inserts a link if not exists and returns it.
func (r *LinkRepository) GetOrCreate(ctx context.Context, url string) (*link.Link, error) {
	l := &link.Link{}
	err := r.db.QueryRow(ctx,
		`INSERT INTO links (url) VALUES ($1)
		 ON CONFLICT (url) DO UPDATE SET url = EXCLUDED.url
		 RETURNING id, url, updated_at`,
		url,
	).Scan(&l.ID, &l.URL, &l.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("get or create link: %w", err)
	}
	return l, nil
}

// FindOldest returns the oldest links.
func (r *LinkRepository) FindOldest(ctx context.Context, limit int) ([]*link.Link, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, url, updated_at FROM links
		 ORDER BY last_checked_at ASC
		 LIMIT $1`,
		limit,
	)
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
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return links, nil
}

func (r *LinkRepository) UpdateCheckedTime(ctx context.Context, id link.ID) error {
	_, err := r.db.Exec(ctx,
		`UPDATE links SET last_checked_at = NOW() WHERE id = $1`,
		id,
	)
	if err != nil {
		return fmt.Errorf("update checked time: %w", err)
	}
	return nil
}

// UpdateTime updates the updated_at field of a link.
func (r *LinkRepository) UpdateTime(ctx context.Context, id link.ID, updatedAt time.Time) error {
	_, err := r.db.Exec(ctx,
		`UPDATE links SET updated_at = $1 WHERE id = $2`,
		updatedAt, id,
	)
	if err != nil {
		return fmt.Errorf("update link time: %w", err)
	}
	return nil
}

// FindByURL returns a link by url.
func (r *LinkRepository) FindByURL(ctx context.Context, url string) (*link.Link, error) {
	l := &link.Link{}
	err := r.db.QueryRow(ctx,
		`SELECT id, url, updated_at FROM links WHERE url = $1`,
		url,
	).Scan(&l.ID, &l.URL, &l.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, link.ErrNotFound
		}
		return nil, fmt.Errorf("find link by url: %w", err)
	}
	return l, nil
}
