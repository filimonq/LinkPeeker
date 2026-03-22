package squirrel

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/botuser"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// UserRepository is a PostgreSQL implementation using squirrel.
type UserRepository struct {
	db *pgxpool.Pool
}

// NewUserRepository creates a new squirrel UserRepository.
func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}

// FindByID returns a user by Telegram ID.
func (r *UserRepository) FindByID(ctx context.Context, id botuser.ID) (*botuser.User, error) {
	sql, args, err := psql.Select("id", "username", "created_at").
		From("users").
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	u := &botuser.User{}
	err = r.db.QueryRow(ctx, sql, args...).Scan(&u.ID, &u.Username, &u.CreatedAt)
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
	sql, args, err := psql.Insert("users").
		Columns("id", "username").
		Values(u.ID, u.Username).
		Suffix("ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username").
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	if _, err = r.db.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("save user: %w", err)
	}
	return nil
}
