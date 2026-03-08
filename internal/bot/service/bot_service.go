package service

import (
	"context"
	"errors"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/user"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service/state"
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

// LinkInfo represents a tracked link.
type LinkInfo struct {
	ID   int64
	URL  string
	Tags []string
}

// ScrapperClient defines operations for scrapper service.
type ScrapperClient interface {
	RegisterChat(ctx context.Context, chatID int64) error
	DeleteChat(ctx context.Context, chatID int64) error
	AddLink(ctx context.Context, chatID int64, url string, tags []string) (*LinkInfo, error)
	RemoveLink(ctx context.Context, chatID int64, url string) (*LinkInfo, error)
	ListLinks(ctx context.Context, chatID int64) ([]*LinkInfo, error)
}

// ErrLinkAlreadyExists is returned when link is already tracked.
var ErrLinkAlreadyExists = errors.New("link already tracked")

// ErrLinkNotFound is returned when link is not found.
var ErrLinkNotFound = errors.New("link not found")

// ErrChatNotFound is returned when chat is not found.
var ErrChatNotFound = errors.New("chat not found")

type botService struct {
	handlers map[domain.CommandType]CommandHandler
	storage  *state.Storage
}

// NewBotService creates a new BotService with provided handlers.
func NewBotService(hList []CommandHandler, storage *state.Storage) BotService {
	hMap := make(map[domain.CommandType]CommandHandler)
	for _, h := range hList {
		hMap[h.CommandType()] = h
	}
	return &botService{handlers: hMap, storage: storage}
}

func (s *botService) HandleCommand(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	if cmd.Type != domain.CommandMessage {
		s.storage.Reset(cmd.ChatID)
	}

	handler, ok := s.handlers[cmd.Type]
	if !ok {
		return domain.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Неизвестная команда. Воспользуйтесь /help.",
		}, nil
	}
	return handler.Handle(ctx, cmd)
}
