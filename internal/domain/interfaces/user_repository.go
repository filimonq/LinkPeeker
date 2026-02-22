package interfaces

import (
	"context"
	"errors"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

var (
	ErrUserNotFound = errors.New("user not found")
	// ErrUserExists   = errors.New("user already exists")
)

type UserRepository interface {
	FindByID(ctx context.Context, id models.UserID) (*models.User, error)
	Save(ctx context.Context, user *models.User) error
}
