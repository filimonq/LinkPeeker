// Package domain contains domain entities.
package domain

import "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/user"

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
	From     user.ID
	ChatID   int64
	FromUser string
}

// CommandMeta holds metadata for Telegram command registration.
type CommandMeta struct {
	Name        string
	Description string
}
