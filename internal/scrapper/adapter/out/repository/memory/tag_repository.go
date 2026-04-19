package memory

import (
	"context"
	"sync"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

type tagKey struct {
	chatID int64
	linkID link.ID
}

// TagRepository is an in-memory implementation of service.TagRepository.
type TagRepository struct {
	mu   sync.RWMutex
	tags map[tagKey][]string
}

// NewTagRepository creates a new in-memory tag repository.
func NewTagRepository() *TagRepository {
	return &TagRepository{tags: make(map[tagKey][]string)}
}

// Add adds a tag to a subscription.
func (r *TagRepository) Add(_ context.Context, chatID int64, linkID link.ID, tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := tagKey{chatID, linkID}
	r.tags[key] = append(r.tags[key], tag)
	return nil
}

// Remove removes a tag from a subscription.
func (r *TagRepository) Remove(_ context.Context, chatID int64, linkID link.ID, tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := tagKey{chatID, linkID}
	for i, t := range r.tags[key] {
		if t == tag {
			r.tags[key] = append(r.tags[key][:i], r.tags[key][i+1:]...)
			return nil
		}
	}
	return nil
}

// FindBySubscription returns all tags for a subscription.
func (r *TagRepository) FindBySubscription(_ context.Context, chatID int64, linkID link.ID) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tags[tagKey{chatID, linkID}], nil
}

// FindByChat returns all tags for all subscriptions of a chat grouped by link ID.
func (r *TagRepository) FindByChat(_ context.Context, chatID int64) (map[link.ID][]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[link.ID][]string)
	for k, tags := range r.tags {
		if k.chatID == chatID {
			result[k.linkID] = append(result[k.linkID], tags...)
		}
	}
	return result, nil
}
