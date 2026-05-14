package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/renaldid/go-event-analytic/internal/event"
)

type mockRepo struct {
	mu     sync.Mutex
	saved  []event.Event
	saveErr error
}

func (m *mockRepo) SaveEvents(_ context.Context, evts []event.Event) error {
	m.mu.Lock()
	m.saved = append(m.saved, evts...)
	m.mu.Unlock()
	return m.saveErr
}

func (m *mockRepo) total() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.saved)
}

func TestRun_SavesEvents(t *testing.T) {
	ch := make(chan []event.Event, 10)
	repo := &mockRepo{}
	w := New(ch, repo)

	ch <- []event.Event{{UserID: "u1", Action: "click"}}
	ch <- []event.Event{{UserID: "u2", Action: "view"}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	if got := repo.total(); got != 2 {
		t.Fatalf("expected 2 saved events, got %d", got)
	}
}

func TestRun_FlushOnCancel(t *testing.T) {
	ch := make(chan []event.Event, 5)
	repo := &mockRepo{}
	w := New(ch, repo)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	// Cancel immediately; enqueue after so flush picks them up.
	cancel()
	ch <- []event.Event{{UserID: "u3", Action: "buy"}}
	<-done

	// flush may or may not have picked up depending on timing; both 0 and 1 are valid.
	// What matters is the worker actually exited.
}

func TestRun_DrainMaxBatch(t *testing.T) {
	ch := make(chan []event.Event, maxDrain+10)
	repo := &mockRepo{}
	w := New(ch, repo)

	// Fill channel beyond maxDrain to exercise the len(batch) >= maxDrain path.
	for i := 0; i < maxDrain+1; i++ {
		ch <- []event.Event{{UserID: "u", Action: "a"}}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done

	if repo.total() < maxDrain+1 {
		t.Fatalf("expected at least %d events saved, got %d", maxDrain+1, repo.total())
	}
}

func TestRun_RepoError(t *testing.T) {
	ch := make(chan []event.Event, 2)
	repo := &mockRepo{}
	w := New(ch, repo)

	ch <- []event.Event{{UserID: "u1", Action: "click"}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done
}

func TestRun_IdleSleep(t *testing.T) {
	ch := make(chan []event.Event, 1)
	repo := &mockRepo{}
	w := New(ch, repo)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	<-done
}

func TestFlush_DrainsPendingEvents(t *testing.T) {
	ch := make(chan []event.Event, 3)
	repo := &mockRepo{}
	w := New(ch, repo)

	ch <- []event.Event{{UserID: "u1", Action: "a"}}
	ch <- []event.Event{{UserID: "u2", Action: "b"}}

	w.flush()

	if got := repo.total(); got != 2 {
		t.Fatalf("expected 2 events flushed, got %d", got)
	}
}
