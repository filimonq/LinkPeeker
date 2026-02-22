package models

type CommandType string

const (
	CommandStart   CommandType = "/start"
	CommandHelp    CommandType = "/help"
	CommandUnknown CommandType = "unknown"
)

type Command struct {
	Type     CommandType
	Payload  string
	From     UserID
	ChatID   int64
	FromUser string
}

type CommandMeta struct {
	Name        string
	Description string
}
