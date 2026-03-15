package handlers

import (
	"context"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service/state"
)

// UntrackHandler handles the /untrack command.
type UntrackHandler struct {
	storage *state.Storage
}

// NewUntrackHandler creates a new UntrackHandler.
func NewUntrackHandler(storage *state.Storage) *UntrackHandler {
	return &UntrackHandler{storage: storage}
}

// Meta returns command metadata for Telegram registration.
func (h *UntrackHandler) Meta() domain.CommandMeta {
	return domain.CommandMeta{Name: "untrack", Description: "Прекратить отслеживание ссылки"}
}

// CommandType returns the command type this handler processes.
func (h *UntrackHandler) CommandType() domain.CommandType { return domain.CommandUntrack }

// Handle processes the /untrack command.
func (h *UntrackHandler) Handle(_ context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	h.storage.Set(cmd.ChatID, state.WaitingUntrackURL)
	return domain.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   "Пришли ссылку которую хочешь удалить:",
	}, nil
}
