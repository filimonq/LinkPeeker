// Package bothttp provides HTTP adapter for bot.
package bothttp

// LinkUpdateRequest is the incoming request body from scrapper.
type LinkUpdateRequest struct {
	ID          int64   `json:"id"`
	URL         string  `json:"url"`
	Description string  `json:"description"`
	TgChatIDs   []int64 `json:"tgChatIds"`
}

// APIErrorResponse is returned on bad requests.
type APIErrorResponse struct {
	Description string `json:"description"`
}
