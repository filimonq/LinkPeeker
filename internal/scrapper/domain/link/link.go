// Package link contains link domain entity.
package link

import "time"

// ID is a unique identifier for a tracked link.
type ID int64

// Link represents a tracked resource.
type Link struct {
	ID        ID
	URL       string
	Tags      []string
	Filters   []string
	ChatID    int64
	CreatedAt time.Time
	UpdatedAt time.Time
}
