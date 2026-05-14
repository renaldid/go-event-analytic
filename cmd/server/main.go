// Package main is the entry point for the go-event-analytic gRPC server.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/renaldid/go-event-analytic/internal/event"
	"github.com/renaldid/go-event-analytic/internal/handler"
	"github.com/renaldid/go-event-analytic/internal/repo"
	"github.com/renaldid/go-event-analytic/internal/worker"
	pb "github.com/renaldid/go-event-analytic/proto"
	"google.golang.org/grpc"
)

const (
	chanCapacity = 100_000
	defaultAddr  = ":50051"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/analytics?sslmode=disable"
	}
	addr := os.Getenv("GRPC_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	ch := make(chan []event.Event, chanCapacity)

	r := repo.New(pool)
	w := worker.New(ch, r)
	h := handler.New(ch)

	go w.Run(ctx)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	pb.RegisterEventServiceServer(srv, h)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	log.Printf("gRPC server listening on %s", addr)
	return srv.Serve(lis)
}
