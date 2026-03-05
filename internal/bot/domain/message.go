package domain

// OutgoingMessage represents a message to be sent to a chat.
type OutgoingMessage struct {
	ChatID int64
	Text   string
}
