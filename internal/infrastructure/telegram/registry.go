package telegram

import (
	"context"
	"fmt"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

// Registry registers bot commands in Telegram UI.
type Registry struct {
	api *tgbotapi.BotAPI
}

// NewRegistry creates a new Registry instance.
func NewRegistry(api *tgbotapi.BotAPI) *Registry {
	return &Registry{api: api}
}

// Register calls setMyCommands Telegram API method.
func (r *Registry) Register(_ context.Context, commands []domain.CommandMeta) error {
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
