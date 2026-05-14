package handler

import (
	"context"
	"testing"
	"time"

	"github.com/renaldid/go-event-analytic/internal/event"
	pb "github.com/renaldid/go-event-analytic/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func makeHandler(cap int) (*EventHandler, chan []event.Event) {
	ch := make(chan []event.Event, cap)
	return New(ch), ch
}

func TestIngestEvent_Valid(t *testing.T) {
	h, ch := makeHandler(1)
	ts := timestamppb.New(time.Now())
	resp, err := h.IngestEvent(context.Background(), &pb.EventRequest{
		Event: &pb.Event{UserId: "u1", Action: "click", Category: "ui", Value: 1, Timestamp: ts},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatal("expected success")
	}
	evts := <-ch
	if len(evts) != 1 || evts[0].UserID != "u1" || evts[0].Action != "click" {
		t.Fatalf("unexpected event: %+v", evts)
	}
}

func TestIngestEvent_NoTimestamp(t *testing.T) {
	h, ch := makeHandler(1)
	_, err := h.IngestEvent(context.Background(), &pb.EventRequest{
		Event: &pb.Event{UserId: "u1", Action: "click"},
	})
	if err != nil {
		t.Fatal(err)
	}
	evts := <-ch
	if !evts[0].Timestamp.IsZero() {
		t.Fatal("expected zero timestamp")
	}
}

func TestIngestEvent_NilEvent(t *testing.T) {
	h, _ := makeHandler(1)
	_, err := h.IngestEvent(context.Background(), &pb.EventRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestIngestEvent_EmptyUserID(t *testing.T) {
	h, _ := makeHandler(1)
	_, err := h.IngestEvent(context.Background(), &pb.EventRequest{
		Event: &pb.Event{Action: "click"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestIngestEvent_EmptyAction(t *testing.T) {
	h, _ := makeHandler(1)
	_, err := h.IngestEvent(context.Background(), &pb.EventRequest{
		Event: &pb.Event{UserId: "u1"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestIngestEvent_ChannelFull(t *testing.T) {
	h, _ := makeHandler(0)
	_, err := h.IngestEvent(context.Background(), &pb.EventRequest{
		Event: &pb.Event{UserId: "u1", Action: "click"},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
}

func TestIngestEvents_Valid(t *testing.T) {
	h, ch := makeHandler(1)
	resp, err := h.IngestEvents(context.Background(), &pb.BatchEventRequest{
		Events: []*pb.Event{
			{UserId: "u1", Action: "click"},
			{UserId: "u2", Action: "view"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatal("expected success")
	}
	evts := <-ch
	if len(evts) != 2 {
		t.Fatalf("expected 2 events, got %d", len(evts))
	}
}

func TestIngestEvents_InvalidEvent(t *testing.T) {
	h, _ := makeHandler(1)
	_, err := h.IngestEvents(context.Background(), &pb.BatchEventRequest{
		Events: []*pb.Event{
			{UserId: "u1", Action: "click"},
			{UserId: "", Action: "view"},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestIngestEvents_ChannelFull(t *testing.T) {
	h, _ := makeHandler(0)
	_, err := h.IngestEvents(context.Background(), &pb.BatchEventRequest{
		Events: []*pb.Event{{UserId: "u1", Action: "click"}},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
}
