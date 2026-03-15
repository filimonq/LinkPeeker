// Package state provides conversation state management for the bot.
package state

// State represents the current conversation state of a chat.
type State int

const (
	// Idle means no active conversation.
	Idle State = iota
	// WaitingURL means bot is waiting for a URL to track.
	WaitingURL
	// WaitingTags means bot is waiting for tags for the tracked URL.
	WaitingTags
	// WaitingUntrackURL means bot is waiting for a URL to untrack.
	WaitingUntrackURL
)
