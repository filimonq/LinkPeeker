package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

const (
	priorityRankLow    = 1
	priorityRankMedium = 2
	priorityRankHigh   = 3
)

type Grouper struct {
	publisher Publisher
	window    time.Duration

	mu      sync.Mutex
	entries map[int64]*groupEntry
}

type groupEntry struct {
	updates []pkgdomain.LinkUpdate
	timer   *time.Timer
}

func NewGrouper(window time.Duration, publisher Publisher) *Grouper {
	return &Grouper{
		publisher: publisher,
		window:    window,
		entries:   make(map[int64]*groupEntry),
	}
}

func (g *Grouper) SendUpdate(_ context.Context, update pkgdomain.LinkUpdate) error {
	if len(update.TgChatIDs) == 0 {
		return nil
	}

	for _, chatID := range update.TgChatIDs {
		g.enqueue(chatID, cloneForChat(update, chatID))
	}

	return nil
}

func (g *Grouper) Close(ctx context.Context) error {
	g.mu.Lock()
	pending := make(map[int64][]pkgdomain.LinkUpdate, len(g.entries))
	for chatID, entry := range g.entries {
		if entry.timer != nil {
			entry.timer.Stop()
		}
		pending[chatID] = append([]pkgdomain.LinkUpdate(nil), entry.updates...)
	}
	g.entries = make(map[int64]*groupEntry)
	g.mu.Unlock()

	for chatID, updates := range pending {
		if err := g.publishGrouped(ctx, chatID, updates); err != nil {
			return err
		}
	}

	return nil
}

func (g *Grouper) enqueue(chatID int64, update pkgdomain.LinkUpdate) {
	g.mu.Lock()
	defer g.mu.Unlock()

	entry, ok := g.entries[chatID]
	if !ok {
		entry = &groupEntry{}
		g.entries[chatID] = entry
	}

	entry.updates = append(entry.updates, update)
	if entry.timer == nil {
		entry.timer = time.AfterFunc(g.window, func() {
			g.flush(chatID)
		})
	}
}

func (g *Grouper) flush(chatID int64) {
	g.mu.Lock()
	entry, ok := g.entries[chatID]
	if !ok {
		g.mu.Unlock()
		return
	}

	updates := append([]pkgdomain.LinkUpdate(nil), entry.updates...)
	delete(g.entries, chatID)
	g.mu.Unlock()

	_ = g.publishGrouped(context.Background(), chatID, updates)
}

func (g *Grouper) publishGrouped(ctx context.Context, chatID int64, updates []pkgdomain.LinkUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	grouped := buildGroupedUpdate(chatID, updates)
	if err := g.publisher.SendUpdate(ctx, grouped); err != nil {
		return fmt.Errorf("publish grouped update: %w", err)
	}

	return nil
}

func buildGroupedUpdate(chatID int64, updates []pkgdomain.LinkUpdate) pkgdomain.LinkUpdate {
	if len(updates) == 1 {
		return updates[0]
	}

	grouped := updates[0]
	grouped.TgChatIDs = []int64{chatID}
	grouped.Priority = maxPriority(updates)
	grouped.Description = joinDescriptions(updates)

	return grouped
}

func joinDescriptions(updates []pkgdomain.LinkUpdate) string {
	var sb strings.Builder

	for i, update := range updates {
		_, _ = fmt.Fprintf(&sb, "%d. %s", i+1, update.Description)
		if i < len(updates)-1 {
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

func maxPriority(updates []pkgdomain.LinkUpdate) string {
	best := priorityLow

	for _, update := range updates {
		if priorityRank(update.Priority) > priorityRank(best) {
			best = update.Priority
		}
	}

	return best
}

func priorityRank(priority string) int {
	switch priority {
	case priorityHigh:
		return priorityRankHigh
	case priorityMedium:
		return priorityRankMedium
	default:
		return priorityRankLow
	}
}

func cloneForChat(update pkgdomain.LinkUpdate, chatID int64) pkgdomain.LinkUpdate {
	cloned := update
	cloned.TgChatIDs = []int64{chatID}
	return cloned
}
