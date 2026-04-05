package memory

import (
	"context"
	"sync"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// TagRepository is an in-memory implementation of service.TagRepository.
type TagRepository struct {
	mu   sync.RWMutex
	tags map[link.ID][]string
}

// NewTagRepository creates a new in-memory tag repository.
func NewTagRepository() *TagRepository {
	return &TagRepository{tags: make(map[link.ID][]string)}
}

// Add adds a tag to a link.
func (r *TagRepository) Add(_ context.Context, linkID link.ID, tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tags[linkID] = append(r.tags[linkID], tag)
	return nil
}

// Remove removes a tag from a link.
func (r *TagRepository) Remove(_ context.Context, linkID link.ID, tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, t := range r.tags[linkID] {
		if t == tag {
			r.tags[linkID] = append(r.tags[linkID][:i], r.tags[linkID][i+1:]...)
			return nil
		}
	}
	return nil
}

// FindByLinkID returns all tags for a link.
func (r *TagRepository) FindByLinkID(_ context.Context, linkID link.ID) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tags[linkID], nil
}
