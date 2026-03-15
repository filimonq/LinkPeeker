package handlers

import (
	"context"
	"fmt"
	"strings"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
)

// ListHandler handles the /list command.
type ListHandler struct {
	scrapper service.ScrapperClient
}

// NewListHandler creates a new ListHandler.
func NewListHandler(scrapper service.ScrapperClient) *ListHandler {
	return &ListHandler{scrapper: scrapper}
}

// Meta returns command metadata.
func (h *ListHandler) Meta() domain.CommandMeta {
	return domain.CommandMeta{Name: "list", Description: "Показать список отслеживаемых ссылок"}
}

// CommandType returns the command type.
func (h *ListHandler) CommandType() domain.CommandType {
	return domain.CommandList
}

// Handle processes the /list command.
func (h *ListHandler) Handle(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	links, err := h.scrapper.ListLinks(ctx, cmd.ChatID)
	if err != nil {
		return domain.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Не удалось получить список ссылок. Попробуйте позже.",
		}, nil
	}

	tag := strings.TrimSpace(cmd.Payload)

	if tag != "" {
		filtered := make([]*service.LinkInfo, 0)
		for _, l := range links {
			for _, t := range l.Tags {
				if t == tag {
					filtered = append(filtered, l)
					break
				}
			}
		}
		links = filtered
	}

	if len(links) == 0 {
		return domain.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Вы пока ничего не отслеживаете. Используйте /track.",
		}, nil
	}

	var sb strings.Builder
	sb.WriteString("Ваши отслеживаемые ссылки:\n\n")
	for i, l := range links {
		fmt.Fprintf(&sb, "%d. %s", i+1, l.URL)
		if len(l.Tags) > 0 {
			fmt.Fprintf(&sb, " (теги: %s)", strings.Join(l.Tags, ", "))
		}
		sb.WriteString("\n")
	}

	return domain.OutgoingMessage{ChatID: cmd.ChatID, Text: sb.String()}, nil
}
