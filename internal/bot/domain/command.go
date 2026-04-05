// Package domain contains domain entities.
package domain

import "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/botuser"

// CommandType represents a bot command string.
type CommandType string

// Supported command types.
const (
	CommandStart   CommandType = "/start"
	CommandHelp    CommandType = "/help"
	CommandTrack   CommandType = "/track"
	CommandUntrack CommandType = "/untrack"
	CommandList    CommandType = "/list"
	CommandCancel  CommandType = "/cancel"
	CommandUnknown CommandType = "unknown"
	CommandMessage CommandType = "message"
)

// Command represents a parsed incoming bot command.
type Command struct {
	Type     CommandType
	Payload  string
	From     botuser.ID
	ChatID   int64
	FromUser string
}

// CommandMeta holds metadata for Telegram command registration.
type CommandMeta struct {
	Name        string
	Description string
}
