package handlers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/user"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
)

// StartHandler handles the /start command.
type StartHandler struct {
	repo     service.UserRepository
	scrapper service.ScrapperClient
}

// NewStartHandler creates a new StartHandler.
func NewStartHandler(repo service.UserRepository, scrapper service.ScrapperClient) *StartHandler {
	return &StartHandler{repo: repo, scrapper: scrapper}
}

// Meta returns command metadata for Telegram registration.
func (h *StartHandler) Meta() domain.CommandMeta {
	return domain.CommandMeta{Name: "start", Description: "Начать работу с ботом"}
}

// CommandType returns the command type this handler processes.
func (h *StartHandler) CommandType() domain.CommandType { return domain.CommandStart }

// Handle processes the /start command.
func (h *StartHandler) Handle(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	_, err := h.repo.FindByID(ctx, cmd.From)

	if err != nil {
		if !errors.Is(err, user.ErrNotFound) {
			return domain.OutgoingMessage{}, fmt.Errorf("failed to check user existence: %w", err)
		}

		newUser := &user.User{
			ID:        cmd.From,
			Username:  cmd.FromUser,
			CreatedAt: time.Now(),
		}

		if err := h.repo.Save(ctx, newUser); err != nil {
			return domain.OutgoingMessage{}, fmt.Errorf("failed to register new user: %w", err)
		}

		if err := h.scrapper.RegisterChat(ctx, cmd.ChatID); err != nil {
			return domain.OutgoingMessage{}, fmt.Errorf("failed to register chat: %w", err)
		}

		return domain.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Добро пожаловать!",
		}, nil
	}

	return domain.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   "С возвращением!",
	}, nil
}
