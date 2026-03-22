package pgxrepo

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/botuser"
)

// UserRepository is a PostgreSQL implementation using raw SQL.
type UserRepository struct {
	db *pgxpool.Pool
}

// NewUserRepository creates a new SQL UserRepository.
func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}

// FindByID returns a user by Telegram ID.
func (r *UserRepository) FindByID(ctx context.Context, id botuser.ID) (*botuser.User, error) {
	u := &botuser.User{}
	err := r.db.QueryRow(ctx,
		"SELECT id, username, created_at FROM users WHERE id = $1",
		id,
	).Scan(&u.ID, &u.Username, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, botuser.ErrNotFound
		}
		return nil, fmt.Errorf("find user: %w", err)
	}
	return u, nil
}

// Save inserts or updates a user.
func (r *UserRepository) Save(ctx context.Context, u *botuser.User) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO users (id, username) VALUES ($1, $2)
		 ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username`,
		u.ID, u.Username,
	)
	if err != nil {
		return fmt.Errorf("save user: %w", err)
	}
	return nil
}
