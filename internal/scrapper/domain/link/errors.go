package link

import "errors"

// ErrNotFound is returned when link is not found.
var ErrNotFound = errors.New("link not found")

// ErrAlreadyExists is returned when link is already tracked.
var ErrAlreadyExists = errors.New("link already tracked")

// ErrInvalidURL is returned when the provided URL is not valid.
var ErrInvalidURL = errors.New("invalid url")
