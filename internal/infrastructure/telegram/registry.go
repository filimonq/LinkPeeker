package telegram

import (
	"context"
	"fmt"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

type Registry struct {
	api *tgbotapi.BotAPI
}

func NewRegistry(api *tgbotapi.BotAPI) *Registry {
	return &Registry{api: api}
}

func (r *Registry) Register(_ context.Context, commands []models.CommandMeta) error {
	tgCmds := make([]tgbotapi.BotCommand, len(commands))
	for i, c := range commands {
		tgCmds[i] = tgbotapi.BotCommand{
			Command:     c.Name,
			Description: c.Description,
		}
	}

	if _, err := r.api.Request(tgbotapi.NewSetMyCommands(tgCmds...)); err != nil {
		return fmt.Errorf("set my commands: %w", err)
	}
	return nil
}
