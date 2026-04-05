// Package memory provides in-memory repository implementations.
package memory

import (
	"context"
	"sync"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/botuser"
)

// UserRepository is an in-memory implementation of user repository.
type UserRepository struct {
	mu    sync.RWMutex
	users map[botuser.ID]*botuser.User
}

// NewUserRepository creates a new in-memory user repository.
func NewUserRepository() *UserRepository {
	return &UserRepository{
		users: make(map[botuser.ID]*botuser.User),
	}
}

// FindByID returns a user by ID.
func (r *UserRepository) FindByID(_ context.Context, id botuser.ID) (*botuser.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	u, ok := r.users[id]
	if !ok {
		return nil, botuser.ErrNotFound
	}
	return u, nil
}

// Save stores a user.
func (r *UserRepository) Save(_ context.Context, u *botuser.User) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.users[u.ID] = u
	return nil
}
