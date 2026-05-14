// Package repo provides the PostgreSQL implementation of event.Repository.
package repo

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/renaldid/go-event-analytic/internal/event"
)

const chunkSize = 10_000

// pgxQuerier is satisfied by *pgxpool.Pool and by test fakes.
type pgxQuerier interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

// PostgresRepo persists events using pgx COPY protocol.
type PostgresRepo struct {
	db pgxQuerier
}

// New returns a PostgresRepo backed by the given pgxQuerier.
func New(db pgxQuerier) *PostgresRepo {
	return &PostgresRepo{db: db}
}

var eventColumns = []string{"user_id", "action", "category", "value", "occurred_at"}

// SaveEvents inserts events in chunks using COPY for high throughput.
func (r *PostgresRepo) SaveEvents(ctx context.Context, events []event.Event) error {
	for _, chunk := range event.Chunk(events, chunkSize) {
		if err := r.copyChunk(ctx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (r *PostgresRepo) copyChunk(ctx context.Context, events []event.Event) error {
	rows := make([][]any, len(events))
	for i, e := range events {
		rows[i] = []any{e.UserID, e.Action, e.Category, e.Value, e.Timestamp}
	}
	_, err := r.db.CopyFrom(ctx, pgx.Identifier{"events"}, eventColumns, pgx.CopyFromRows(rows))
	return err
}
