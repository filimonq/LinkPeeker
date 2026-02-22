package handlers

import (
	"context"
	"errors"
	"fmt"
	"time"

	repository "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/interfaces"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

// StartHandler handles the /start command.
type StartHandler struct {
	repo repository.UserRepository
}

// NewStartHandler creates a new StartHandler.
func NewStartHandler(repo repository.UserRepository) *StartHandler {
	return &StartHandler{repo: repo}
}

// Meta returns command metadata for Telegram registration.
func (h *StartHandler) Meta() models.CommandMeta {
	return models.CommandMeta{Name: "start", Description: "Начать работу с ботом"}
}

// CommandType returns the command type this handler processes.
func (h *StartHandler) CommandType() models.CommandType { return models.CommandStart }

// Handle processes the /start command.
func (h *StartHandler) Handle(ctx context.Context, cmd models.Command) (models.OutgoingMessage, error) {
	_, err := h.repo.FindByID(ctx, cmd.From)

	if err != nil {
		if !errors.Is(err, repository.ErrUserNotFound) {
			return models.OutgoingMessage{}, fmt.Errorf("failed to check user existence: %w", err)
		}

		newUser := &models.User{
			ID:        cmd.From,
			Username:  cmd.FromUser,
			CreatedAt: time.Now(),
		}

		if err := h.repo.Save(ctx, newUser); err != nil {
			return models.OutgoingMessage{}, fmt.Errorf("failed to register new user: %w", err)
		}

		return models.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Добро пожаловать!",
		}, nil
	}

	return models.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   "С возвращением!",
	}, nil
}
