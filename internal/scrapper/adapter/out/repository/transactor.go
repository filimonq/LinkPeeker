package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txKey struct{}

// Transactor manages database transactions.
type Transactor struct {
	db *pgxpool.Pool
}

// NewTransactor creates a new Transactor.
func NewTransactor(db *pgxpool.Pool) *Transactor {
	return &Transactor{db: db}
}

// WithinTransaction runs fn inside a transaction.
func (t *Transactor) WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := t.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err = fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// ExtractTx extracts transaction from context if exists.
func ExtractTx(ctx context.Context) pgx.Tx {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return nil
}
