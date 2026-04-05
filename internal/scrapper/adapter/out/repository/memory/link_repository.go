package memory

import (
	"context"
	"sync"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// LinkRepository is an in-memory implementation of service.LinkRepository.
type LinkRepository struct {
	mu    sync.RWMutex
	links map[int64][]*link.Link
	seq   int64
}

// NewLinkRepository creates a new in-memory link repository.
func NewLinkRepository() *LinkRepository {
	return &LinkRepository{
		links: make(map[int64][]*link.Link),
	}
}

// Add adds a link to the repository.
func (r *LinkRepository) Add(_ context.Context, l *link.Link) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, existing := range r.links[l.ChatID] {
		if existing.URL == l.URL {
			return link.ErrAlreadyExists
		}
	}

	r.seq++
	l.ID = link.ID(r.seq)
	l.CreatedAt = time.Now()
	l.UpdatedAt = time.Now()
	r.links[l.ChatID] = append(r.links[l.ChatID], l)
	return nil
}

// Remove removes a link from the repository.
func (r *LinkRepository) Remove(_ context.Context, chatID int64, url string) (*link.Link, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	links := r.links[chatID]
	for i, l := range links {
		if l.URL == url {
			r.links[chatID] = append(links[:i], links[i+1:]...)
			return l, nil
		}
	}
	return nil, link.ErrNotFound
}

// FindByChatID returns links for a given chat ID with pagination.
func (r *LinkRepository) FindByChatID(_ context.Context, chatID int64, limit, offset int) ([]*link.Link, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	all := r.links[chatID]
	if offset >= len(all) {
		return nil, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], nil
}

// FindAll returns all links with pagination.
func (r *LinkRepository) FindAll(_ context.Context, limit, offset int) ([]*link.Link, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var all []*link.Link
	for _, links := range r.links {
		all = append(all, links...)
	}
	if offset >= len(all) {
		return nil, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], nil
}

// FindByURL returns a link by chatID and url.
func (r *LinkRepository) FindByURL(_ context.Context, chatID int64, url string) (*link.Link, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, l := range r.links[chatID] {
		if l.URL == url {
			return l, nil
		}
	}
	return nil, link.ErrNotFound
}

// Exists checks if a link exists in the repository.
func (r *LinkRepository) Exists(_ context.Context, chatID int64, url string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, l := range r.links[chatID] {
		if l.URL == url {
			return true, nil
		}
	}
	return false, nil
}

// UpdateTime updates the UpdatedAt field of a link.
func (r *LinkRepository) UpdateTime(_ context.Context, id link.ID, updatedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, links := range r.links {
		for _, l := range links {
			if l.ID == id {
				l.UpdatedAt = updatedAt
				return nil
			}
		}
	}
	return link.ErrNotFound
}
