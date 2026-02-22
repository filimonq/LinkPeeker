// Package handlers contains bot command handlers.
package handlers

import (
	"context"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

// HelpHandler handles the /help command.
type HelpHandler struct{}

// NewHelpHandler creates a new HelpHandler.
func NewHelpHandler() *HelpHandler {
	return &HelpHandler{}
}

// Meta returns command metadata for Telegram registration.
func (h *HelpHandler) Meta() models.CommandMeta {
	return models.CommandMeta{Name: "help", Description: "Список доступных команд"}
}

// CommandType returns the command type this handler processes.
func (h *HelpHandler) CommandType() models.CommandType {
	return models.CommandHelp
}

// Handle processes the /help command and returns available commands list.
func (h *HelpHandler) Handle(_ context.Context, cmd models.Command) (models.OutgoingMessage, error) {
	return models.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   "Доступные команды:\n/start - начать работу\n/help - вывести список доступных команд",
	}, nil
}
