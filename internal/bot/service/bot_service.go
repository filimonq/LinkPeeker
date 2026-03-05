// Package service contains business logic and use cases.
package service

import (
	"context"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/user"
)

// UserRepository defines storage operations for users.
type UserRepository interface {
	FindByID(ctx context.Context, id user.ID) (*user.User, error)
	Save(ctx context.Context, u *user.User) error
}

// CommandHandler processes a specific bot command.
type CommandHandler interface {
	Handle(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error)
	CommandType() domain.CommandType
	Meta() domain.CommandMeta
}

// BotService handles incoming bot commands.
type BotService interface {
	HandleCommand(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error)
}

type botService struct {
	handlers map[domain.CommandType]CommandHandler
}

// NewBotService creates a new BotService with provided handlers.
func NewBotService(hList []CommandHandler) BotService {
	hMap := make(map[domain.CommandType]CommandHandler)
	for _, h := range hList {
		hMap[h.CommandType()] = h
	}
	return &botService{handlers: hMap}
}

func (s *botService) HandleCommand(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	handler, ok := s.handlers[cmd.Type]
	if !ok {
		return domain.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Неизвестная команда. Воспользуйтесь /help.",
		}, nil
	}

	return handler.Handle(ctx, cmd)
}
