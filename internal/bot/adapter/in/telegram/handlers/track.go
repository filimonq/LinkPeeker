package handlers

import (
	"context"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service/state"
)

// TrackHandler handles the /track command.
type TrackHandler struct {
	storage *state.Storage
}

// NewTrackHandler creates a new TrackHandler.
func NewTrackHandler(storage *state.Storage) *TrackHandler {
	return &TrackHandler{storage: storage}
}

// Meta returns command metadata for Telegram registration.
func (h *TrackHandler) Meta() domain.CommandMeta {
	return domain.CommandMeta{Name: "track", Description: "Начать отслеживание ссылки"}
}

// CommandType returns the command type this handler processes.
func (h *TrackHandler) CommandType() domain.CommandType { return domain.CommandTrack }

// Handle processes the /track command.
func (h *TrackHandler) Handle(_ context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	h.storage.Set(cmd.ChatID, state.WaitingURL)
	return domain.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   "Пришли ссылку на репозиторий GitHub или вопрос StackOverflow:",
	}, nil
}
