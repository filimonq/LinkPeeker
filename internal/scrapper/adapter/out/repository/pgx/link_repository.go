package pgxrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// LinkRepository is a PostgreSQL implementation using raw SQL.
type LinkRepository struct {
	db *pgxpool.Pool
}

// NewLinkRepository creates a new pgx LinkRepository.
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

	err = tx.QueryRow(ctx,
		`INSERT INTO links (url, chat_id) VALUES ($1, $2)
		 RETURNING id, created_at, updated_at`,
		l.URL, l.ChatID,
	).Scan(&l.ID, &l.CreatedAt, &l.UpdatedAt)
	if err != nil {
		if repository.IsUniqueViolation(err) {
			return link.ErrAlreadyExists
		}
		return fmt.Errorf("insert link: %w", err)
	}

	for _, tag := range l.Tags {
		if _, err = tx.Exec(ctx,
			"INSERT INTO link_tags (link_id, tag) VALUES ($1, $2)",
			l.ID, tag,
		); err != nil {
			return fmt.Errorf("insert tag: %w", err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// Remove deletes a link by chatID and url.
func (r *LinkRepository) Remove(ctx context.Context, chatID int64, url string) (*link.Link, error) {
	l := &link.Link{}
	err := r.db.QueryRow(ctx,
		`DELETE FROM links WHERE chat_id = $1 AND url = $2
		 RETURNING id, url, chat_id, created_at, updated_at`,
		chatID, url,
	).Scan(&l.ID, &l.URL, &l.ChatID, &l.CreatedAt, &l.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, link.ErrNotFound
		}
		return nil, fmt.Errorf("delete link: %w", err)
	}
	return l, nil
}

// FindByChatID returns all links for a chat with their tags with pagination.
func (r *LinkRepository) FindByChatID(ctx context.Context, chatID int64, limit, offset int) ([]*link.Link, error) {
	rows, err := r.db.Query(ctx,
		`SELECT l.id, l.url, l.chat_id, l.created_at, l.updated_at,
		        COALESCE(array_agg(lt.tag) FILTER (WHERE lt.tag IS NOT NULL), '{}')
		 FROM links l
		 LEFT JOIN link_tags lt ON lt.link_id = l.id
		 WHERE l.chat_id = $1
		 GROUP BY l.id
		 LIMIT $2 OFFSET $3`,
		chatID, limit, offset,
	)
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

// FindAll returns all links with their tags with pagination.
func (r *LinkRepository) FindAll(ctx context.Context, limit, offset int) ([]*link.Link, error) {
	rows, err := r.db.Query(ctx,
		`SELECT l.id, l.url, l.chat_id, l.created_at, l.updated_at,
		        COALESCE(array_agg(lt.tag) FILTER (WHERE lt.tag IS NOT NULL), '{}')
		 FROM links l
		 LEFT JOIN link_tags lt ON lt.link_id = l.id
		 GROUP BY l.id
		 LIMIT $1 OFFSET $2`,
		limit, offset,
	)
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

// FindByURL returns a link by chatID and url.
func (r *LinkRepository) FindByURL(ctx context.Context, chatID int64, url string) (*link.Link, error) {
	l := &link.Link{}
	err := r.db.QueryRow(ctx,
		`SELECT id, url, chat_id, created_at, updated_at
		 FROM links WHERE chat_id = $1 AND url = $2`,
		chatID, url,
	).Scan(&l.ID, &l.URL, &l.ChatID, &l.CreatedAt, &l.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, link.ErrNotFound
		}
		return nil, fmt.Errorf("find link by url: %w", err)
	}
	return l, nil
}

// Exists checks if a link exists for a given chat and url.
func (r *LinkRepository) Exists(ctx context.Context, chatID int64, url string) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM links WHERE chat_id = $1 AND url = $2)",
		chatID, url,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check link exists: %w", err)
	}
	return exists, nil
}

// UpdateTime updates the updated_at field of a link.
func (r *LinkRepository) UpdateTime(ctx context.Context, id link.ID, updatedAt time.Time) error {
	tag, err := r.db.Exec(ctx,
		"UPDATE links SET updated_at = $1 WHERE id = $2",
		updatedAt, id,
	)
	if err != nil {
		return fmt.Errorf("update link time: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return link.ErrNotFound
	}
	return nil
}
