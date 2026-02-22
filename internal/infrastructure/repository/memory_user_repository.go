// Package memory provides in-memory repository implementations.
package memory

import (
	"context"
	"sync"

	repository "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/interfaces"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

type userRepository struct {
	mu    sync.RWMutex
	users map[models.UserID]*models.User
}

// NewUserRepository creates a new in-memory user repository.
func NewUserRepository() repository.UserRepository {
	return &userRepository{
		users: make(map[models.UserID]*models.User),
	}
}

func (r *userRepository) FindByID(_ context.Context, id models.UserID) (*models.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	user, ok := r.users[id]
	if !ok {
		return nil, repository.ErrUserNotFound
	}

	return user, nil
}

func (r *userRepository) Save(_ context.Context, user *models.User) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.users[user.ID] = user
	return nil
}
