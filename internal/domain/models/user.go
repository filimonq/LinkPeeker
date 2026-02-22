package models

import "time"

type UserID int64

type User struct {
	ID        UserID
	Username  string
	CreatedAt time.Time
}
