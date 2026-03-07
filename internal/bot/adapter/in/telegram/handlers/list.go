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

func NewListHandler(scrapper service.ScrapperClient) *ListHandler {
	return &ListHandler{scrapper: scrapper}
}

func (h *ListHandler) Meta() domain.CommandMeta {
	return domain.CommandMeta{Name: "list", Description: "Показать список отслеживаемых ссылок"}
}

func (h *ListHandler) CommandType() domain.CommandType {
	return domain.CommandList
}

func (h *ListHandler) Handle(ctx context.Context, cmd domain.Command) (domain.OutgoingMessage, error) {
	links, err := h.scrapper.ListLinks(ctx, cmd.ChatID)
	if err != nil {
		return domain.OutgoingMessage{
			ChatID: cmd.ChatID,
			Text:   "Не удалось получить список ссылок. Попробуйте позже.",
		}, nil
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

	return domain.OutgoingMessage{
		ChatID: cmd.ChatID,
		Text:   sb.String(),
	}, nil
}
