package models

import "time"

// UserID is a unique identifier for a Telegram user.
type UserID int64

// User represents a registered bot user.
type User struct {
	ID        UserID
	Username  string
	CreatedAt time.Time
}
