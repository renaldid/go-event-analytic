// Package handler implements the gRPC EventService handlers.
package handler

import (
	"context"

	"github.com/renaldid/go-event-analytic/internal/event"
	pb "github.com/renaldid/go-event-analytic/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EventHandler implements pb.EventServiceServer.
type EventHandler struct {
	pb.UnimplementedEventServiceServer
	ch chan<- []event.Event
}

// New returns an EventHandler that enqueues validated events onto ch.
func New(ch chan<- []event.Event) *EventHandler {
	return &EventHandler{ch: ch}
}

// IngestEvent validates and enqueues a single event.
func (h *EventHandler) IngestEvent(_ context.Context, req *pb.EventRequest) (*pb.IngestResponse, error) {
	if req.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "event must not be nil")
	}
	evts, err := convertAndValidate([]*pb.Event{req.GetEvent()})
	if err != nil {
		return nil, err
	}
	return h.enqueue(evts)
}

// IngestEvents validates and enqueues a batch of events.
func (h *EventHandler) IngestEvents(_ context.Context, req *pb.BatchEventRequest) (*pb.IngestResponse, error) {
	evts, err := convertAndValidate(req.GetEvents())
	if err != nil {
		return nil, err
	}
	return h.enqueue(evts)
}

func (h *EventHandler) enqueue(evts []event.Event) (*pb.IngestResponse, error) {
	select {
	case h.ch <- evts:
		return &pb.IngestResponse{Success: true, Message: "enqueued"}, nil
	default:
		return nil, status.Error(codes.ResourceExhausted, "service busy")
	}
}

func convertAndValidate(protoEvents []*pb.Event) ([]event.Event, error) {
	evts := make([]event.Event, 0, len(protoEvents))
	for i, pe := range protoEvents {
		if pe.GetUserId() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "event[%d]: user_id must not be empty", i)
		}
		if pe.GetAction() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "event[%d]: action must not be empty", i)
		}
		e := event.Event{
			UserID:   pe.GetUserId(),
			Action:   pe.GetAction(),
			Category: pe.GetCategory(),
			Value:    pe.GetValue(),
		}
		if pe.GetTimestamp() != nil {
			e.Timestamp = pe.GetTimestamp().AsTime()
		}
		evts = append(evts, e)
	}
	return evts, nil
}
