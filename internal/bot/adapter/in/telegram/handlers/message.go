package handlers

import (
	"context"
	"errors"
	"strings"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service/state"
)

// MessageHandler handles plain text messages based on conversation state.
type MessageHandler struct {
	scrapper service.ScrapperClient
	storage  *state.Storage
}

// NewMessageHandler creates a new MessageHandler.
func NewMessageHandler(scrapper service.ScrapperClient, storage *state.Storage) *MessageHandler {
	return &MessageHandler{scrapper: scrapper, storage: storage}
}

// Meta returns empty metadata — not a registerable command.
func (h *MessageHandler) Meta() domain.CommandMeta { return domain.CommandMeta{} }

// CommandType returns the command type this handler processes.
func (h *MessageHandler) CommandType() domain.CommandType { return domain.CommandMessage }

// Handle processes plain text based on current conversation state.
func (h *MessageHandler) Handle(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	switch h.storage.Get(cmd.ChatID) {
	case state.WaitingURL:
		return h.handleTrackURL(cmd)
	case state.WaitingTags:
		return h.handleTags(ctx, cmd)
	case state.WaitingUntrackURL:
		return h.handleUntrackURL(ctx, cmd)
	default:
		return domain.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Используй /help чтобы увидеть доступные команды",
		}, nil
	}
}

func (h *MessageHandler) handleTrackURL(cmd domain.Command) (domain.OutgoingMessage, error) {
	url := strings.TrimSpace(cmd.Payload)
	if url == "" {
		return domain.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Ссылка не может быть пустой. Попробуй ещё раз или /cancel",
		}, nil
	}
	h.storage.SetURL(cmd.ChatID, url)
	h.storage.Set(cmd.ChatID, state.WaitingTags)
	return domain.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   "Пришли теги через запятую или /cancel чтобы пропустить:",
	}, nil
}

func (h *MessageHandler) handleTags(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	url := h.storage.GetURL(cmd.ChatID)
	tags := parseTags(cmd.Payload)

	_, err := h.scrapper.AddLink(ctx, cmd.ChatID, url, tags)
	h.storage.Reset(cmd.ChatID)
	if err != nil {
		if errors.Is(err, service.ErrLinkAlreadyExists) {
			return domain.OutgoingMessage{ChatID: cmd.ChatID, Text: "Ссылка уже отслеживается"}, nil
		}
		return domain.OutgoingMessage{ChatID: cmd.ChatID, Text: "Не удалось добавить ссылку"}, nil
	}
	return domain.OutgoingMessage{ChatID: cmd.ChatID, Text: "Ссылка добавлена ✅"}, nil
}

func (h *MessageHandler) handleUntrackURL(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	url := strings.TrimSpace(cmd.Payload)
	if url == "" {
		return domain.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Ссылка не может быть пустой. Попробуй ещё раз или /cancel",
		}, nil
	}
	_, err := h.scrapper.RemoveLink(ctx, cmd.ChatID, url)
	h.storage.Reset(cmd.ChatID)
	if err != nil {
		return domain.OutgoingMessage{ChatID: cmd.ChatID, Text: "Ссылка не найдена"}, nil
	}
	return domain.OutgoingMessage{ChatID: cmd.ChatID, Text: "Ссылка удалена ✅"}, nil
}

func parseTags(input string) []string {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	parts := strings.Split(input, ",")
	tags := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			tags = append(tags, t)
		}
	}
	return tags
}
