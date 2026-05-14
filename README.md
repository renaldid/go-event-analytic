# go-event-analytic

A high-throughput gRPC microservice for real-time user event ingestion and analytics processing.
Supports single-event and batch ingestion up to one million events, with optimistic ACK and PostgreSQL persistence via COPY.

[![CI](https://github.com/renaldid/go-event-analytic/actions/workflows/ci.yml/badge.svg)](https://github.com/renaldid/go-event-analytic/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/renaldid/go-event-analytic/branch/master/graph/badge.svg)](https://codecov.io/gh/renaldid/go-event-analytic)
[![Go Report Card](https://goreportcard.com/badge/github.com/renaldid/go-event-analytic)](https://goreportcard.com/report/github.com/renaldid/go-event-analytic)

## Features

- gRPC service with two RPCs: `IngestEvent` (single) and `IngestEvents` (batch)
- Field validation: `user_id` and `action` are required on every event
- Optimistic ACK: client receives success immediately after enqueue, not after DB write
- Buffered in-memory channel (capacity 100,000) decouples ingestion from persistence
- Backpressure: returns `ResourceExhausted` when the channel is full
- Background worker drains up to 10,000 events per cycle and writes in bulk using PostgreSQL COPY
- Graceful shutdown: worker flushes remaining events on SIGTERM before exit
- N+1 query detection via [n1detect](https://github.com/renaldid/n1detect) in CI (static analysis only)

## Architecture

```
Client
  |
  v
gRPC Handler  -->  validate (user_id, action)  -->  chan []Event (cap=100k)  -->  ACK success
                                                             |
                                                             v
                                                    Background Worker
                                                         |
                                                         v
                                                  PostgresRepo.SaveEvents
                                                         |
                                                         v
                                                  PostgreSQL (COPY protocol)
```

The handler enqueues immediately and ACKs the client. The worker drains the channel in batches
and uses `pgx` COPY for maximum insert throughput. On context cancellation (SIGTERM), the worker
flushes whatever remains in the channel before exiting.

## Proto API

```protobuf
service EventService {
  rpc IngestEvent(EventRequest) returns (IngestResponse);
  rpc IngestEvents(BatchEventRequest) returns (IngestResponse);
}

message Event {
  string user_id  = 1;
  string action   = 2;
  string category = 3;
  int32  value    = 4;
  google.protobuf.Timestamp timestamp = 5;
}

message IngestResponse { bool success = 1; string message = 2; }
```

## Requirements

- Go 1.23+
- PostgreSQL 14+

## Getting Started

Create the events table:

```sql
CREATE TABLE events (
  id          BIGSERIAL PRIMARY KEY,
  user_id     TEXT        NOT NULL,
  action      TEXT        NOT NULL,
  category    TEXT        NOT NULL DEFAULT '',
  value       INTEGER     NOT NULL DEFAULT 0,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Set environment variables and run:

```bash
export DATABASE_URL="postgres://user:pass@localhost:5432/analytics?sslmode=disable"
export GRPC_ADDR=":50051"
go run ./cmd/server
```

## Configuration

| Variable       | Default                                                    | Description             |
| -------------- | ---------------------------------------------------------- | ----------------------- |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/analytics...` | PostgreSQL connection   |
| `GRPC_ADDR`    | `:50051`                                                   | gRPC listen address     |

## Development

```bash
# Run tests with race detector
go test -race ./...

# Run tests with coverage
go test -race -coverprofile=coverage.out -covermode=atomic \
  $(go list ./... | grep -v '/proto' | grep -v '/cmd/')
go tool cover -html=coverage.out

# Lint
golangci-lint run
```

## Project Structure

```
cmd/server/         entry point, dependency wiring
internal/event/     domain Event type, Repository interface, Chunk helper
internal/handler/   gRPC request handler
internal/worker/    background drain-and-persist goroutine
internal/repo/      PostgreSQL repository (pgx COPY)
proto/              protobuf definitions and generated Go stubs
```

## Dependencies

| Package                        | Purpose                       |
| ------------------------------ | ----------------------------- |
| `google.golang.org/grpc`       | gRPC server and generated stubs |
| `google.golang.org/protobuf`   | Protobuf runtime              |
| `github.com/jackc/pgx/v5`      | PostgreSQL driver (COPY)      |
| `github.com/renaldid/n1detect` | N+1 query static analysis (CI only) |

## License

MIT
