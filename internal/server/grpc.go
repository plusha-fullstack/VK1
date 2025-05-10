package server

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"

	"VK1/pkg/subpub"
	"VK1/proto"
)

type PubSubServerImpl struct {
	proto.UnimplementedPubSubServer
	sp     subpub.SubPub
	logger *log.Logger
}

func NewPubSubServer(sp subpub.SubPub, logger *log.Logger) *PubSubServerImpl {
	return &PubSubServerImpl{
		sp:     sp,
		logger: logger,
	}
}

func (s *PubSubServerImpl) Publish(ctx context.Context, req *proto.PublishRequest) (*emptypb.Empty, error) {
	s.logger.Printf("Received Publish request for key '%s', data: '%s'", req.Key, req.Data)

	if req.Key == "" {
		s.logger.Printf("Publish failed: key is empty")
		return nil, status.Errorf(codes.InvalidArgument, "key cannot be empty")
	}
	if req.Data == "" {
		s.logger.Printf("Publish failed: data is empty")
		return nil, status.Errorf(codes.InvalidArgument, "data cannot be empty")
	}

	err := s.sp.Publish(req.Key, req.Data)
	if err != nil {
		s.logger.Printf("Failed to publish message for key '%s': %v", req.Key, err)
		if err.Error() == "subpub is closed" {
			return nil, status.Errorf(codes.Unavailable, "service is shutting down or subpub is closed: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to publish message: %v", err)
	}

	s.logger.Printf("Successfully published to key '%s'", req.Key)
	return &emptypb.Empty{}, nil
}

func (s *PubSubServerImpl) Subscribe(req *proto.SubscribeRequest, stream proto.PubSub_SubscribeServer) error {

	if p, ok := peer.FromContext(stream.Context()); ok {
		s.logger.Printf("Received Subscribe request for key '%s' from client %s", req.Key, p.Addr)
	} else {
		s.logger.Printf("Received Subscribe request for key '%s' from unknown client", req.Key)
	}

	if req.Key == "" {
		s.logger.Printf("Subscribe failed: key is empty")
		return status.Errorf(codes.InvalidArgument, "key cannot be empty")
	}

	eventChan := make(chan *proto.Event, 10)

	handler := func(msg interface{}) {
		s.logger.Printf("SubPub delivered message for key '%s' to gRPC handler", req.Key)
		data, ok := msg.(string)
		if !ok {
			s.logger.Printf("Error: received non-string message for key '%s': %T. Skipping.", req.Key, msg)
			return
		}

		event := &proto.Event{Data: data}

		select {
		case eventChan <- event:
			s.logger.Printf("Queued event for key '%s' to be sent via gRPC stream", req.Key)
		default:
			s.logger.Printf("Warning: eventChan full for key '%s'. Message dropped for this subscriber.", req.Key)
		}
	}

	subscription, err := s.sp.Subscribe(req.Key, handler)
	if err != nil {
		s.logger.Printf("Failed to subscribe to subpub for key '%s': %v", req.Key, err)
		if err.Error() == "subpub is closed" {
			return status.Errorf(codes.Unavailable, "service is shutting down or subpub is closed: %v", err)
		}
		return status.Errorf(codes.Internal, "failed to subscribe: %v", err)
	}
	s.logger.Printf("Successfully subscribed to subpub for key '%s'", req.Key)

	defer func() {
		s.logger.Printf("Unsubscribing from key '%s' for gRPC stream", req.Key)
		subscription.Unsubscribe()
		close(eventChan)
		s.logger.Printf("Unsubscribed and cleaned up for key '%s'", req.Key)
	}()

	for {
		select {
		case <-stream.Context().Done():
			s.logger.Printf("Client for key '%s' disconnected or context cancelled: %v", req.Key, stream.Context().Err())
			return stream.Context().Err()
		case event, ok := <-eventChan:
			if !ok {
				s.logger.Printf("Event channel closed for key '%s'. Terminating stream.", req.Key)
				return status.Errorf(codes.Aborted, "event channel closed, subscription ending")
			}
			s.logger.Printf("Sending event on key '%s' to gRPC client: %s", req.Key, event.Data)
			if err := stream.Send(event); err != nil {
				s.logger.Printf("Error sending event to client for key '%s': %v", req.Key, err)
				return status.Errorf(codes.Unavailable, "failed to send event to client: %v", err)
			}
			s.logger.Printf("Successfully sent event for key '%s'", req.Key)
		}
	}
}
