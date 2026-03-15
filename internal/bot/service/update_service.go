package service

import (
	"context"
	"fmt"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

// MessageSender sends text messages to Telegram chats.
type MessageSender interface {
	Send(chatID int64, text string)
}

// UpdateService handles incoming link update notifications from scrapper.
type UpdateService interface {
	SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error
}

type updateService struct {
	sender MessageSender
}

// NewUpdateService creates a new UpdateService.
func NewUpdateService(sender MessageSender) UpdateService {
	return &updateService{sender: sender}
}

func (s *updateService) SendUpdate(_ context.Context, update pkgdomain.LinkUpdate) error {
	text := fmt.Sprintf("🔔 Обновление по ссылке:\n%s\n\n%s", update.URL, update.Description)
	for _, chatID := range update.TgChatIDs {
		s.sender.Send(chatID, text)
	}
	return nil
}
