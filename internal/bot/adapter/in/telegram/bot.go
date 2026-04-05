// Package telegram provides Telegram bot infrastructure.
package telegram

import (
	"context"
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain/botuser"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
)

// Bot handles Telegram updates via long polling.
type Bot struct {
	api     *tgbotapi.BotAPI
	service service.BotService
	logger  *slog.Logger
}

// NewBot creates a new Bot instance.
func NewBot(api *tgbotapi.BotAPI, service service.BotService, logger *slog.Logger) *Bot {
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

// Send implements service.MessageSender — sends a text message to a chat.
func (b *Bot) Send(chatID int64, text string) {
	b.sendMessage(domain.OutgoingMessage{ChatID: chatID, Text: text})
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

func (b *Bot) parseCommand(msg *tgbotapi.Message) domain.Command {
	var cmdType domain.CommandType
	var payload string

	if msg.IsCommand() {
		rawCmd := "/" + msg.Command()
		payload = msg.CommandArguments()

		switch domain.CommandType(rawCmd) {
		case domain.CommandStart, domain.CommandHelp,
			domain.CommandTrack, domain.CommandUntrack,
			domain.CommandList, domain.CommandCancel:
			cmdType = domain.CommandType(rawCmd)
		case domain.CommandUnknown, domain.CommandMessage:
			cmdType = domain.CommandUnknown

		default:
			cmdType = domain.CommandUnknown
		}
	} else {
		cmdType = domain.CommandMessage
		payload = msg.Text
	}

	return domain.Command{
		Type:     cmdType,
		Payload:  payload,
		ChatID:   msg.Chat.ID,
		From:     botuser.ID(msg.From.ID),
		FromUser: msg.From.UserName,
	}
}

func (b *Bot) sendMessage(msg domain.OutgoingMessage) {
	m := tgbotapi.NewMessage(msg.ChatID, msg.Text)
	if _, err := b.api.Send(m); err != nil {
		b.logger.Error("send message failed",
			slog.Int64("chat_id", msg.ChatID),
			slog.String("error", err.Error()),
		)
	}
}
