// Package domain contains shared domain models.
package domain

import "time"

// LinkUpdate represents a notification about link changes
// sent from scrapper to bot.
type LinkUpdate struct {
	ID          int64   `json:"id"`
	URL         string  `json:"url"`
	Description string  `json:"description"`
	TgChatIDs   []int64 `json:"tgChatIds"`
	Author      string  `json:"author,omitempty"`
	Priority    string  `json:"priority,omitempty"`
}

// UpdateInfo contains details about a detected change at a URL.
type UpdateInfo struct {
	// UpdatedAt is the time of the detected change.
	UpdatedAt time.Time
	// Title is the question title (SO) or PR/Issue name (GitHub).
	Title string
	// Author is the username who created the update.
	Author string
	// CreatedAt is when this specific item (answer/comment/PR/issue) was created.
	CreatedAt time.Time
	// Preview is the first 200 characters of the body.
	Preview string
}
