package user

import "context"

// Repository defines storage operations for users.
type Repository interface {
	FindByID(ctx context.Context, id ID) (*User, error)
	Save(ctx context.Context, user *User) error
}
