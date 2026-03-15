package state

import "sync"

// Storage stores conversation states per chat.
type Storage struct {
	mu     sync.RWMutex
	states map[int64]State
	urls   map[int64]string
}

// NewStorage creates a new Storage.
func NewStorage() *Storage {
	return &Storage{
		states: make(map[int64]State),
		urls:   make(map[int64]string),
	}
}

// Get returns the current state for a chat.
func (s *Storage) Get(chatID int64) State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.states[chatID]
}

// Set sets the state for a chat.
func (s *Storage) Set(chatID int64, st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[chatID] = st
}

// SetURL saves URL while bot is waiting for tags.
func (s *Storage) SetURL(chatID int64, url string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.urls[chatID] = url
}

// GetURL returns the saved URL for a chat.
func (s *Storage) GetURL(chatID int64) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.urls[chatID]
}

// Reset clears state and URL for a chat.
func (s *Storage) Reset(chatID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.states, chatID)
	delete(s.urls, chatID)
}
