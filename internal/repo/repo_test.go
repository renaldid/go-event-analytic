package repo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/renaldid/go-event-analytic/internal/event"
)

type fakeQuerier struct {
	callCount int
	rowCounts []int
	err       error
}

func (f *fakeQuerier) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, rowSrc pgx.CopyFromSource) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	var n int64
	for rowSrc.Next() {
		_, _ = rowSrc.Values()
		n++
	}
	f.callCount++
	f.rowCounts = append(f.rowCounts, int(n))
	return n, rowSrc.Err()
}

func makeEvents(n int) []event.Event {
	evts := make([]event.Event, n)
	for i := range evts {
		evts[i] = event.Event{UserID: "u", Action: "a", Timestamp: time.Now()}
	}
	return evts
}

func TestSaveEvents_Empty(t *testing.T) {
	q := &fakeQuerier{}
	r := New(q)
	if err := r.SaveEvents(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if q.callCount != 0 {
		t.Fatalf("expected 0 CopyFrom calls, got %d", q.callCount)
	}
}

func TestSaveEvents_SingleChunk(t *testing.T) {
	q := &fakeQuerier{}
	r := New(q)
	if err := r.SaveEvents(context.Background(), makeEvents(5)); err != nil {
		t.Fatal(err)
	}
	if q.callCount != 1 {
		t.Fatalf("expected 1 CopyFrom call, got %d", q.callCount)
	}
	if q.rowCounts[0] != 5 {
		t.Fatalf("expected 5 rows, got %d", q.rowCounts[0])
	}
}

func TestSaveEvents_MultipleChunks(t *testing.T) {
	q := &fakeQuerier{}
	r := New(q)
	if err := r.SaveEvents(context.Background(), makeEvents(chunkSize+1)); err != nil {
		t.Fatal(err)
	}
	if q.callCount != 2 {
		t.Fatalf("expected 2 CopyFrom calls, got %d", q.callCount)
	}
	if q.rowCounts[0] != chunkSize {
		t.Fatalf("first chunk: expected %d rows, got %d", chunkSize, q.rowCounts[0])
	}
	if q.rowCounts[1] != 1 {
		t.Fatalf("second chunk: expected 1 row, got %d", q.rowCounts[1])
	}
}

func TestSaveEvents_CopyError(t *testing.T) {
	q := &fakeQuerier{err: errors.New("db error")}
	r := New(q)
	if err := r.SaveEvents(context.Background(), makeEvents(3)); err == nil {
		t.Fatal("expected error")
	}
}
