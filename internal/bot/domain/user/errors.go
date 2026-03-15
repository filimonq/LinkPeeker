package user

import "errors"

// ErrNotFound is returned when user is not found in storage.
var ErrNotFound = errors.New("user not found")
