// Package models contains domain entities.
package models

// CommandType represents a bot command string.
type CommandType string

// Supported command types.
const (
	CommandStart   CommandType = "/start"
	CommandHelp    CommandType = "/help"
	CommandUnknown CommandType = "unknown"
)

// Command represents a parsed incoming bot command.
type Command struct {
	Type     CommandType
	Payload  string
	From     UserID
	ChatID   int64
	FromUser string
}

// CommandMeta holds metadata for Telegram command registration.
type CommandMeta struct {
	Name        string
	Description string
}
