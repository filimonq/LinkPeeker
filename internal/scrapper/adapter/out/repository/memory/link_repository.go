package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// LinkRepository is an in-memory implementation of service.LinkRepository.
type LinkRepository struct {
	mu          sync.RWMutex
	links       map[link.ID]*link.Link
	linksByURL  map[string]link.ID
	lastChecked map[link.ID]time.Time
	seq         int64
}

// NewLinkRepository creates a new in-memory link repository.
func NewLinkRepository() *LinkRepository {
	return &LinkRepository{
		links:       make(map[link.ID]*link.Link),
		linksByURL:  make(map[string]link.ID),
		lastChecked: make(map[link.ID]time.Time),
	}
}

// GetOrCreate returns existing link or creates a new one.
func (r *LinkRepository) GetOrCreate(_ context.Context, url string) (*link.Link, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if id, ok := r.linksByURL[url]; ok {
		return r.links[id], nil
	}

	r.seq++
	id := link.ID(r.seq)
	l := &link.Link{
		ID:        id,
		URL:       url,
		UpdatedAt: time.Unix(0, 0),
	}

	r.links[id] = l
	r.linksByURL[url] = id
	r.lastChecked[id] = time.Unix(0, 0)

	return l, nil
}

// FindAll returns all links with pagination.
func (r *LinkRepository) FindAll(_ context.Context, limit, offset int) ([]*link.Link, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	all := make([]*link.Link, 0, len(r.links))
	for _, l := range r.links {
		all = append(all, l)
	}
	if offset >= len(all) {
		return nil, nil
	}
	end := min(offset+limit, len(all))
	return all[offset:end], nil
}

func (r *LinkRepository) FindOldest(_ context.Context, limit int) ([]*link.Link, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	type item struct {
		id link.ID
		t  time.Time
	}

	var items []item
	for id, t := range r.lastChecked {
		items = append(items, item{id, t})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].t.Before(items[j].t)
	})

	n := min(limit, len(items))

	result := make([]*link.Link, 0, n)
	for i := range n {
		result = append(result, r.links[items[i].id])
	}
	return result, nil
}

func (r *LinkRepository) UpdateCheckedTime(_ context.Context, id link.ID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.lastChecked[id] = time.Now()
	return nil
}

func (r *LinkRepository) UpdateTime(_ context.Context, id link.ID, updatedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if l, ok := r.links[id]; ok {
		l.UpdatedAt = updatedAt
	}
	return nil
}

func (r *LinkRepository) FindByURL(_ context.Context, url string) (*link.Link, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	id, ok := r.linksByURL[url]
	if !ok {
		return nil, link.ErrNotFound
	}
	return r.links[id], nil
}
