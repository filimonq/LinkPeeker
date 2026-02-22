package handlers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/interfaces"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

type StartHandler struct {
	repo interfaces.UserRepository
}

func NewStartHandler(repo interfaces.UserRepository) *StartHandler {
	return &StartHandler{repo: repo}
}

func (h *StartHandler) Meta() models.CommandMeta {
	return models.CommandMeta{Name: "start", Description: "Начать работу с ботом"}
}

func (h *StartHandler) CommandType() models.CommandType { return models.CommandStart }

func (h *StartHandler) Handle(ctx context.Context, cmd models.Command) (models.OutgoingMessage, error) {
	_, err := h.repo.FindByID(ctx, cmd.From)

	if err != nil {
		if !errors.Is(err, interfaces.ErrUserNotFound) {
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
