package handlers

import (
	"context"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

type HelpHandler struct{}

func NewHelpHandler() *HelpHandler {
	return &HelpHandler{}
}

func (h *HelpHandler) Meta() models.CommandMeta {
	return models.CommandMeta{Name: "help", Description: "Список доступных команд"}
}

func (h *HelpHandler) CommandType() models.CommandType {
	return models.CommandHelp
}

func (h *HelpHandler) Handle(ctx context.Context, cmd models.Command) (models.OutgoingMessage, error) {
	return models.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   "Доступные команды:\n/start - начать работу\n/help - вывести список доступных команд",
	}, nil
}
