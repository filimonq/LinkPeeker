package squirrel

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
)

// OutboxRepository is a squirrel-based PostgreSQL implementation of the outbox pattern.
type OutboxRepository struct {
	db *pgxpool.Pool
}

// NewOutboxRepository creates a new squirrel OutboxRepository.
func NewOutboxRepository(db *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// Add inserts a new pending outbox record inside the transaction carried by ctx (if any).
func (r *OutboxRepository) Add(ctx context.Context, payload []byte) error {
	exec := repository.GetExecutor(ctx, r.db)

	query, args, err := psql.Insert("outbox").
		Columns("payload").
		Values(payload).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	if _, err = exec.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("insert outbox entry: %w", err)
	}
	return nil
}

// FetchPending returns up to limit undelivered outbox entries.
func (r *OutboxRepository) FetchPending(ctx context.Context, limit int) ([]*service.OutboxEntry, error) {
	query, args, err := psql.Select("id", "payload").
		From("outbox").
		Where(sq.Eq{"status": "pending"}).
		OrderBy("created_at").
		Limit(uint64(limit)).
		Suffix("FOR UPDATE SKIP LOCKED").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := r.db.Query(ctx, query, args...)
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

// MarkSent flips the entry status to 'sent'. Called only after publisher ACK.
func (r *OutboxRepository) MarkSent(ctx context.Context, id int64) error {
	query, args, err := psql.Update("outbox").
		Set("status", "sent").
		Set("sent_at", sq.Expr("NOW()")).
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	if _, err = r.db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("mark outbox sent: %w", err)
	}
	return nil
}
