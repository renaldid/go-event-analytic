// Package worker provides a background goroutine that drains the event channel
// and persists batches to the repository.
package worker

import (
	"context"
	"time"

	"github.com/renaldid/go-event-analytic/internal/event"
)

const (
	maxDrain  = 10_000
	sleepIdle = 50 * time.Millisecond
)

// Worker drains events from a channel and persists them via repo.
type Worker struct {
	ch   <-chan []event.Event
	repo event.Repository
}

// New returns a Worker that reads from ch and writes through repo.
func New(ch <-chan []event.Event, repo event.Repository) *Worker {
	return &Worker{ch: ch, repo: repo}
}

// Run processes events until ctx is cancelled, then flushes remaining items.
func (w *Worker) Run(ctx context.Context) {
	for {
		batch := w.drain()
		if len(batch) == 0 {
			select {
			case <-ctx.Done():
				w.flush()
				return
			case <-time.After(sleepIdle):
				continue
			}
		}
		_ = w.repo.SaveEvents(ctx, batch)
	}
}

// drain reads up to maxDrain accumulated events from the channel without blocking.
func (w *Worker) drain() []event.Event {
	var batch []event.Event
	for {
		select {
		case evts := <-w.ch:
			batch = append(batch, evts...)
			if len(batch) >= maxDrain {
				return batch
			}
		default:
			return batch
		}
	}
}

// flush drains whatever remains in the channel after context cancellation.
func (w *Worker) flush() {
	for {
		select {
		case evts := <-w.ch:
			_ = w.repo.SaveEvents(context.Background(), evts)
		default:
			return
		}
	}
}
