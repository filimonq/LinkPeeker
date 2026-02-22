// Package application contains business logic and use cases.
package application

import (
	"context"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

// CommandHandler processes a specific bot command.
type CommandHandler interface {
	Handle(ctx context.Context, cmd models.Command) (models.OutgoingMessage, error)
	CommandType() models.CommandType
	Meta() models.CommandMeta
}

// BotService handles incoming bot commands.
type BotService interface {
	HandleCommand(ctx context.Context, cmd models.Command) (models.OutgoingMessage, error)
}

type botService struct {
	handlers map[models.CommandType]CommandHandler
}

// NewBotService creates a new BotService with provided handlers.
func NewBotService(hList []CommandHandler) BotService {
	hMap := make(map[models.CommandType]CommandHandler)
	for _, h := range hList {
		hMap[h.CommandType()] = h
	}
	return &botService{handlers: hMap}
}

func (s *botService) HandleCommand(ctx context.Context, cmd models.Command) (models.OutgoingMessage, error) {
	handler, ok := s.handlers[cmd.Type]
	if !ok {
		return models.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Неизвестная команда. Воспользуйтесь /help.",
		}, nil
	}

	return handler.Handle(ctx, cmd)
}
