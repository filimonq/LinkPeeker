package pgxrepo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
)

// OutboxRepository is a PostgreSQL implementation of the outbox pattern.
type OutboxRepository struct {
	db *pgxpool.Pool
}

func NewOutboxRepository(db *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// Add inserts a pending outbox record inside the transaction carried by ctx.
func (r *OutboxRepository) Add(ctx context.Context, payload []byte) error {
	exec := repository.GetExecutor(ctx, r.db)
	_, err := exec.Exec(ctx,
		`INSERT INTO outbox (payload) VALUES ($1)`,
		payload,
	)
	if err != nil {
		return fmt.Errorf("insert outbox entry: %w", err)
	}
	return nil
}

// FetchPending returns up to limit undelivered rows locked with SKIP LOCKED.
func (r *OutboxRepository) FetchPending(ctx context.Context, limit int) ([]*service.OutboxEntry, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, payload FROM outbox
		 WHERE status = 'pending'
		 ORDER BY created_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch pending outbox: %w", err)
	}
	defer rows.Close()

	var entries []*service.OutboxEntry
	for rows.Next() {
		e := &service.OutboxEntry{}
		if err = rows.Scan(&e.ID, &e.Payload); err != nil {
			return nil, fmt.Errorf("scan outbox entry: %w", err)
		}
		entries = append(entries, e)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox rows error: %w", err)
	}
	return entries, nil
}

// MarkSent flips the entry to 'sent'. Called only after publisher ACK.
func (r *OutboxRepository) MarkSent(ctx context.Context, id int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE outbox SET status = 'sent', sent_at = NOW() WHERE id = $1`,
		id,
	)
	if err != nil {
		return fmt.Errorf("mark outbox sent: %w", err)
	}
	return nil
}
