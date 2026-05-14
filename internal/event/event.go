// Package event defines the core domain types and interfaces for the analytics service.
package event

import (
	"context"
	"time"
)

// Event is the internal domain representation of a single analytics event.
type Event struct {
	UserID    string
	Action    string
	Category  string
	Value     int32
	Timestamp time.Time
}

// Repository persists events to a durable store.
type Repository interface {
	SaveEvents(ctx context.Context, events []Event) error
}

// Chunk splits s into successive sub-slices of at most size elements.
func Chunk[T any](s []T, size int) [][]T {
	if len(s) == 0 {
		return nil
	}
	var out [][]T
	for len(s) > 0 {
		n := size
		if n > len(s) {
			n = len(s)
		}
		out = append(out, s[:n])
		s = s[n:]
	}
	return out
}
