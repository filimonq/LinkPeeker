package pgxrepo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/chat"
)

// ChatRepository is a PostgreSQL implementation using raw SQL.
type ChatRepository struct {
	db *pgxpool.Pool
}

// NewChatRepository creates a new pgx ChatRepository.
func NewChatRepository(db *pgxpool.Pool) *ChatRepository {
	return &ChatRepository{db: db}
}

// Add inserts a new chat.
func (r *ChatRepository) Add(ctx context.Context, id int64) error {
	exec := repository.GetExecutor(ctx, r.db)
	_, err := exec.Exec(ctx, "INSERT INTO chats (chat_id) VALUES ($1)", id)
	if err != nil {
		if repository.IsUniqueViolation(err) {
			return chat.ErrAlreadyExists
		}
		return fmt.Errorf("insert chat: %w", err)
	}
	return nil
}

// Remove deletes a chat by id.
func (r *ChatRepository) Remove(ctx context.Context, id int64) error {
	exec := repository.GetExecutor(ctx, r.db)
	tag, err := exec.Exec(ctx, "DELETE FROM chats WHERE chat_id = $1", id)
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
	exec := repository.GetExecutor(ctx, r.db)
	var exists bool
	err := exec.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM chats WHERE chat_id = $1)", id,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check chat exists: %w", err)
	}
	return exists, nil
}
