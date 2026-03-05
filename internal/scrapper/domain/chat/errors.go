package chat

import "errors"

// ErrNotFound is returned when chat is not found.
var ErrNotFound = errors.New("chat not found")

// ErrAlreadyExists is returned when chat already registered.
var ErrAlreadyExists = errors.New("chat already exists")
