package handlers

import (
	"context"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service/state"
)

// CancelHandler handles the /cancel command.
type CancelHandler struct {
	storage *state.Storage
}

func NewCancelHandler(storage *state.Storage) *CancelHandler {
	return &CancelHandler{storage: storage}
}

func (h *CancelHandler) Meta() domain.CommandMeta {
	return domain.CommandMeta{Name: "cancel", Description: "Отменить текущую операцию"}
}

func (h *CancelHandler) CommandType() domain.CommandType {
	return domain.CommandCancel
}

func (h *CancelHandler) Handle(_ context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	h.storage.Reset(cmd.ChatID)
	return domain.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   "Операция отменена.",
	}, nil
}
