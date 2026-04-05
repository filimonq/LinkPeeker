package squirrel

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/chat"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// ChatRepository is a PostgreSQL implementation using squirrel.
type ChatRepository struct {
	db *pgxpool.Pool
}

// NewChatRepository creates a new squirrel ChatRepository.
func NewChatRepository(db *pgxpool.Pool) *ChatRepository {
	return &ChatRepository{db: db}
}

// Add inserts a new chat.
func (r *ChatRepository) Add(ctx context.Context, id int64) error {
	query, args, err := psql.Insert("chats").Columns("chat_id").Values(id).ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}
	if _, err = r.db.Exec(ctx, query, args...); err != nil {
		if repository.IsUniqueViolation(err) {
			return chat.ErrAlreadyExists
		}
		return fmt.Errorf("insert chat: %w", err)
	}
	return nil
}

// Remove deletes a chat by id.
func (r *ChatRepository) Remove(ctx context.Context, id int64) error {
	query, args, err := psql.Delete("chats").Where(sq.Eq{"chat_id": id}).ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}
	tag, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete chat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return chat.ErrNotFound
	}
	return nil
}

// Exists checks if a chat exists.
func (r *ChatRepository) Exists(ctx context.Context, id int64) (bool, error) {
	inner, args, err := psql.Select("1").From("chats").Where(sq.Eq{"chat_id": id}).ToSql()
	if err != nil {
		return false, fmt.Errorf("build query: %w", err)
	}
	var exists bool
	if err = r.db.QueryRow(ctx, "SELECT EXISTS("+inner+")", args...).Scan(&exists); err != nil {
		return false, fmt.Errorf("check chat exists: %w", err)
	}
	return exists, nil
}
