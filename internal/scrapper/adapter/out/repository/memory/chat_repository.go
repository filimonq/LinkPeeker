// Package memory provides in-memory repository implementations.
package memory

import (
	"context"
	"sync"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/chat"
)

// ChatRepository is an in-memory implementation of service.ChatRepository.
type ChatRepository struct {
	mu    sync.RWMutex
	chats map[int64]struct{}
}

// NewChatRepository creates a new in-memory chat repository.
func NewChatRepository() *ChatRepository {
	return &ChatRepository{
		chats: make(map[int64]struct{}),
	}
}

// Add adds a chat to the repository.
func (r *ChatRepository) Add(_ context.Context, id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.chats[id]; ok {
		return chat.ErrAlreadyExists
	}
	r.chats[id] = struct{}{}
	return nil
}

// Remove removes a chat from the repository.
func (r *ChatRepository) Remove(_ context.Context, id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.chats[id]; !ok {
		return chat.ErrNotFound
	}
	delete(r.chats, id)
	return nil
}

// Exists checks if a chat exists in the repository.
func (r *ChatRepository) Exists(_ context.Context, id int64) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.chats[id]
	return ok, nil
}
