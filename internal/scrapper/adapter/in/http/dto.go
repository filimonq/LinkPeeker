// Package scrapperhttp provides HTTP adapter for scrapper.
package scrapperhttp

// AddLinkRequest represents request body for adding a link.
type AddLinkRequest struct {
	Link string   `json:"link"`
	Tags []string `json:"tags"`
}

// RemoveLinkRequest represents request body for removing a link.
type RemoveLinkRequest struct {
	Link string `json:"link"`
}

// LinkResponse represents a link in HTTP response.
type LinkResponse struct {
	ID   int64    `json:"id"`
	URL  string   `json:"url"`
	Tags []string `json:"tags"`
}

// ListLinksResponse represents response for listing links.
type ListLinksResponse struct {
	Links []LinkResponse `json:"links"`
	Size  int            `json:"size"`
}

// APIErrorResponse represents an error response.
type APIErrorResponse struct {
	Description      string `json:"description"`
	Code             string `json:"code"`
	ExceptionMessage string `json:"exceptionMessage"`
}
