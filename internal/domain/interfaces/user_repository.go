// Package repository defines domain repository interfaces.
package repository

import (
	"context"
	"errors"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

// ErrUserNotFound is returned when user is not found in storage.
var (
	ErrUserNotFound = errors.New("user not found")
	// ErrUserExists   = errors.New("user already exists")
)

// UserRepository defines storage operations for users.
type UserRepository interface {
	FindByID(ctx context.Context, id models.UserID) (*models.User, error)
	Save(ctx context.Context, user *models.User) error
}
