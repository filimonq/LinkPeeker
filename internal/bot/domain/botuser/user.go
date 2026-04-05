// Package botuser contains user domain entity.
package botuser

import "time"

// ID is a unique identifier for a Telegram user.
type ID int64

// User represents a registered bot user.
type User struct {
	ID        ID
	Username  string
	CreatedAt time.Time
}
