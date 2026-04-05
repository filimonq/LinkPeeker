package memory

import (
	"context"
	"sync"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

type subscriptionKey struct {
	chatID int64
	linkID link.ID
}

// SubscriptionRepository is an in-memory implementation of service.SubscriptionRepository.
type SubscriptionRepository struct {
	mu       sync.RWMutex
	subs     map[subscriptionKey]struct{} // chatID+linkID -> exists
	byChat   map[int64][]link.ID          // chatID -> []linkID
	byLink   map[link.ID][]int64          // linkID -> []chatID
	linkRepo *LinkRepository
}

// NewSubscriptionRepository creates a new in-memory subscription repository.
func NewSubscriptionRepository(linkRepo *LinkRepository) *SubscriptionRepository {
	return &SubscriptionRepository{
		subs:     make(map[subscriptionKey]struct{}),
		byChat:   make(map[int64][]link.ID),
		byLink:   make(map[link.ID][]int64),
		linkRepo: linkRepo,
	}
}

// Add adds a subscription.
func (r *SubscriptionRepository) Add(_ context.Context, chatID int64, linkID link.ID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := subscriptionKey{chatID, linkID}
	r.subs[key] = struct{}{}
	r.byChat[chatID] = append(r.byChat[chatID], linkID)
	r.byLink[linkID] = append(r.byLink[linkID], chatID)
	return nil
}

// Remove removes a subscription by chatID and url.
func (r *SubscriptionRepository) Remove(ctx context.Context, chatID int64, url string) (*link.Link, error) {
	l, err := r.linkRepo.FindByURL(ctx, url)
	if err != nil {
		return nil, link.ErrNotFound
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := subscriptionKey{chatID, l.ID}
	if _, ok := r.subs[key]; !ok {
		return nil, link.ErrNotFound
	}

	delete(r.subs, key)

	ids := r.byChat[chatID]
	for i, id := range ids {
		if id == l.ID {
			r.byChat[chatID] = append(ids[:i], ids[i+1:]...)
			break
		}
	}

	chats := r.byLink[l.ID]
	for i, id := range chats {
		if id == chatID {
			r.byLink[l.ID] = append(chats[:i], chats[i+1:]...)
			break
		}
	}

	return l, nil
}

// FindByChatID returns all links for a chat with pagination.
func (r *SubscriptionRepository) FindByChatID(ctx context.Context, chatID int64, limit, offset int) ([]*link.Link, error) {
	r.mu.RLock()
	linkIDs := r.byChat[chatID]
	r.mu.RUnlock()

	if offset >= len(linkIDs) {
		return nil, nil
	}
	end := offset + limit
	if end > len(linkIDs) {
		end = len(linkIDs)
	}

	var links []*link.Link
	for _, id := range linkIDs[offset:end] {
		r.linkRepo.mu.RLock()
		for _, l := range r.linkRepo.links {
			if l.ID == id {
				links = append(links, l)
				break
			}
		}
		r.linkRepo.mu.RUnlock()
	}
	return links, nil
}

// FindChatsByLinkID returns all chat IDs subscribed to a link.
func (r *SubscriptionRepository) FindChatsByLinkID(_ context.Context, linkID link.ID) ([]int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byLink[linkID], nil
}

// Exists checks if a subscription exists.
func (r *SubscriptionRepository) Exists(ctx context.Context, chatID int64, url string) (bool, error) {
	l, err := r.linkRepo.FindByURL(ctx, url)
	if err != nil {
		return false, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.subs[subscriptionKey{chatID, l.ID}]
	return ok, nil
}
