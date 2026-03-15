// Package domain contains shared domain models.
package domain

// LinkUpdate represents a notification about link changes
// sent from scrapper to bot.
type LinkUpdate struct {
	ID          int64   `json:"id"`
	URL         string  `json:"url"`
	Description string  `json:"description"`
	TgChatIDs   []int64 `json:"tgChatIds"`
}
