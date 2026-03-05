// Package handlers contains bot command handlers.
package handlers

import (
	"context"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
)

// HelpHandler handles the /help command.
type HelpHandler struct{}

// NewHelpHandler creates a new HelpHandler.
func NewHelpHandler() *HelpHandler {
	return &HelpHandler{}
}

// Meta returns command metadata for Telegram registration.
func (h *HelpHandler) Meta() domain.CommandMeta {
	return domain.CommandMeta{Name: "help", Description: "Список доступных команд"}
}

// CommandType returns the command type this handler processes.
func (h *HelpHandler) CommandType() domain.CommandType {
	return domain.CommandHelp
}

// Handle processes the /help command and returns available commands list.
func (h *HelpHandler) Handle(_ context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	return domain.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   "Доступные команды:\n/start - начать работу\n/help - вывести список доступных команд",
	}, nil
}
