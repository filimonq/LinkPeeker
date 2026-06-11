package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/observability"
)

const (
	priorityRankLow    = 1
	priorityRankMedium = 2
	priorityRankHigh   = 3
	groupRetryDelay    = 1 * time.Second
)

type Grouper struct {
	publisher Publisher
	window    time.Duration
	logger    *slog.Logger
	metrics   *observability.AgentMetrics

	mu      sync.Mutex
	entries map[int64]*groupEntry
	closed  bool
}

type groupEntry struct {
	updates []pkgdomain.LinkUpdate
	timer   *time.Timer
	ctx     context.Context
}

func NewGrouper(window time.Duration, publisher Publisher, logger *slog.Logger) *Grouper {
	if logger == nil {
		logger = slog.Default()
	}

	return &Grouper{
		publisher: publisher,
		window:    window,
		logger:    logger,
		entries:   make(map[int64]*groupEntry),
	}
}

func WithGrouperMetrics(g *Grouper, metrics *observability.AgentMetrics) {
	if g != nil {
		g.metrics = metrics
	}
}
func (g *Grouper) SendUpdate(ctx context.Context, update pkgdomain.LinkUpdate) error {
	if len(update.TgChatIDs) == 0 {
		return nil
	}

	for _, chatID := range update.TgChatIDs {
		g.enqueue(ctx, chatID, cloneForChat(update, chatID))
	}
	if g.metrics != nil {
		g.metrics.ObservePipelineEvent("group_enqueue", "success")
	}

	return nil
}

func (g *Grouper) Close(ctx context.Context) error {
	g.mu.Lock()
	g.closed = true
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

func (g *Grouper) enqueue(ctx context.Context, chatID int64, update pkgdomain.LinkUpdate) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}

	entry, ok := g.entries[chatID]
	if !ok {
		entry = &groupEntry{}
		g.entries[chatID] = entry
	}

	entry.updates = append(entry.updates, update)
	entry.ctx = ctx
	if entry.timer == nil {
		entry.timer = time.AfterFunc(g.window, func() {
			g.flush(ctx, chatID)
		})
	}
}

func (g *Grouper) flush(fallbackCtx context.Context, chatID int64) {
	g.mu.Lock()
	entry, ok := g.entries[chatID]
	if !ok {
		g.mu.Unlock()
		return
	}

	updates := append([]pkgdomain.LinkUpdate(nil), entry.updates...)
	ctx := entry.ctx
	delete(g.entries, chatID)
	g.mu.Unlock()

	if ctx == nil {
		ctx = fallbackCtx
	}
	if ctx == nil {
		ctx = context.TODO()
	}

	if err := g.publishGrouped(ctx, chatID, updates); err != nil {
		g.logger.Error("failed to publish grouped update",
			slog.Int64("chat_id", chatID),
			slog.String("err", err.Error()),
		)
		g.requeue(ctx, chatID, updates, groupRetryDelay)
	}
}

func (g *Grouper) requeue(ctx context.Context, chatID int64, updates []pkgdomain.LinkUpdate, delay time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}

	entry, ok := g.entries[chatID]
	if !ok {
		entry = &groupEntry{}
		g.entries[chatID] = entry
	}

	entry.updates = append(updates, entry.updates...)
	entry.ctx = ctx
	if entry.timer == nil {
		entry.timer = time.AfterFunc(delay, func() {
			g.flush(ctx, chatID)
		})
	}
}

func (g *Grouper) publishGrouped(ctx context.Context, chatID int64, updates []pkgdomain.LinkUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	start := time.Now()
	grouped := buildGroupedUpdate(chatID, updates)
	if err := g.publisher.SendUpdate(ctx, grouped); err != nil {
		if g.metrics != nil {
			g.metrics.ObserveOperationDuration("grouping", "publish_grouped", time.Since(start))
			g.metrics.ObservePipelineEvent("group_publish", "error")
		}
		return fmt.Errorf("publish grouped update: %w", err)
	}
	if g.metrics != nil {
		g.metrics.ObserveOperationDuration("grouping", "publish_grouped", time.Since(start))
		if len(updates) > 1 {
			g.metrics.ObservePipelineEvent("group_publish", "grouped")
		} else {
			g.metrics.ObservePipelineEvent("group_publish", "single")
		}
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
