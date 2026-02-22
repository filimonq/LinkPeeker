// Package telegram provides Telegram bot infrastructure.
package telegram

import (
	"context"
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain/models"
)

// Bot handles Telegram updates via long polling.
type Bot struct {
	api     *tgbotapi.BotAPI
	service application.BotService
	logger  *slog.Logger
}

// NewBot creates a new Bot instance.
func NewBot(api *tgbotapi.BotAPI, service application.BotService, logger *slog.Logger) *Bot {
	return &Bot{api: api, service: service, logger: logger}
}

// Start begins polling for updates and blocks until ctx is cancelled.
func (b *Bot) Start(ctx context.Context) {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := b.api.GetUpdatesChan(u) // под капотом горутина, которая слушает апдейты от тг и пушит их в канал
	for {
		select {
		case update := <-updates:
			if update.Message == nil {
				continue
			}
			go b.handleUpdate(ctx, update)
		case <-ctx.Done():
			b.logger.Info("bot is shutting down")
			return
		}
	}
}

func (b *Bot) handleUpdate(ctx context.Context, update tgbotapi.Update) {
	if update.Message.From == nil {
		b.logger.Warn("received message without sender")
		return
	}

	cmd := b.parseCommand(update.Message)

	b.logger.Info("received command",
		slog.String("command", string(cmd.Type)),
		slog.String("username", cmd.FromUser),
		slog.Int64("user_id", int64(cmd.From)),
		slog.Int64("chat_id", cmd.ChatID),
	)

	response, err := b.service.HandleCommand(ctx, cmd)
	if err != nil {
		b.logger.Error("handle command failed",
			slog.String("error", err.Error()),
		)
		return
	}

	b.sendMessage(response)
}

func (b *Bot) parseCommand(msg *tgbotapi.Message) models.Command {
	cmdType := models.CommandUnknown

	rawCmd := "/" + msg.Command()
	switch models.CommandType(rawCmd) {
	case models.CommandStart, models.CommandHelp:
		cmdType = models.CommandType(rawCmd)
	}

	return models.Command{
		Type:     cmdType,
		Payload:  msg.CommandArguments(),
		From:     models.UserID(msg.From.ID),
		ChatID:   msg.Chat.ID,
		FromUser: msg.From.UserName,
	}
}

func (b *Bot) sendMessage(msg models.OutgoingMessage) {
	m := tgbotapi.NewMessage(msg.ChatID, msg.Text)
	if _, err := b.api.Send(m); err != nil {
		b.logger.Error("send message failed",
			slog.Int64("chat_id", msg.ChatID),
			slog.String("error", err.Error()),
		)
	}
}
