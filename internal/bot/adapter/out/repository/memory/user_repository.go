// Package memory provides in-memory repository implementations.
package memory

import (
	"context"
	"sync"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/user"
)

type userRepository struct {
	mu    sync.RWMutex
	users map[user.ID]*user.User
}

// NewUserRepository creates a new in-memory user repository.
func NewUserRepository() *userRepository {
	return &userRepository{
		users: make(map[user.ID]*user.User),
	}
}

func (r *userRepository) FindByID(_ context.Context, id user.ID) (*user.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	u, ok := r.users[id]
	if !ok {
		return nil, user.ErrNotFound
	}
	return u, nil
}

func (r *userRepository) Save(_ context.Context, u *user.User) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.users[u.ID] = u
	return nil
}
