// Package grpc provides gRPC adapter for scrapper service.
package grpc

import (
	"context"
	"errors"
	"net"

	googlegrpc "google.golang.org/grpc"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/chat"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
	scrapperpb "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/proto"
)

// Server is the gRPC server for scrapper.
type Server struct {
	scrapperpb.UnimplementedScrapperServiceServer
	svc  service.ScrapperService
	grpc *googlegrpc.Server
}

// NewServer creates a new gRPC Server.
func NewServer(svc service.ScrapperService) *Server {
	g := googlegrpc.NewServer()
	s := &Server{svc: svc, grpc: g}
	scrapperpb.RegisterScrapperServiceServer(g, s)
	return s
}

// Start begins listening for gRPC requests.
func (s *Server) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.grpc.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	s.grpc.GracefulStop()
}

// RegisterChat handles chat registration.
func (s *Server) RegisterChat(ctx context.Context, req *scrapperpb.RegisterChatRequest) (*scrapperpb.RegisterChatResponse, error) {
	if err := s.svc.RegisterChat(ctx, req.ChatId); err != nil {
		if errors.Is(err, chat.ErrAlreadyExists) {
			return nil, status.Error(codes.AlreadyExists, "chat already registered")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &scrapperpb.RegisterChatResponse{}, nil
}

// DeleteChat handles chat deletion.
func (s *Server) DeleteChat(ctx context.Context, req *scrapperpb.DeleteChatRequest) (*scrapperpb.DeleteChatResponse, error) {
	if err := s.svc.DeleteChat(ctx, req.ChatId); err != nil {
		if errors.Is(err, chat.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "chat not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &scrapperpb.DeleteChatResponse{}, nil
}

// AddLink handles link addition.
func (s *Server) AddLink(ctx context.Context, req *scrapperpb.AddLinkRequest) (*scrapperpb.LinkResponse, error) {
	l, err := s.svc.AddLink(ctx, req.ChatId, req.Url, req.Tags)
	if err != nil {
		if errors.Is(err, chat.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "chat not found")
		}
		if errors.Is(err, link.ErrAlreadyExists) {
			return nil, status.Error(codes.AlreadyExists, "link already exists")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &scrapperpb.LinkResponse{
		Id:   int64(l.ID),
		Url:  l.URL,
		Tags: l.Tags,
	}, nil
}

// RemoveLink handles link removal.
func (s *Server) RemoveLink(ctx context.Context, req *scrapperpb.RemoveLinkRequest) (*scrapperpb.LinkResponse, error) {
	l, err := s.svc.RemoveLink(ctx, req.ChatId, req.Url)
	if err != nil {
		if errors.Is(err, chat.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "chat not found")
		}
		if errors.Is(err, link.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "link not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &scrapperpb.LinkResponse{
		Id:   int64(l.ID),
		Url:  l.URL,
		Tags: l.Tags,
	}, nil
}

// ListLinks handles link listing.
func (s *Server) ListLinks(ctx context.Context, req *scrapperpb.ListLinksRequest) (*scrapperpb.ListLinksResponse, error) {
	links, err := s.svc.ListLinks(ctx, req.ChatId)
	if err != nil {
		if errors.Is(err, chat.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "chat not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	pbLinks := make([]*scrapperpb.LinkResponse, len(links))
	for i, l := range links {
		pbLinks[i] = &scrapperpb.LinkResponse{
			Id:   int64(l.ID),
			Url:  l.URL,
			Tags: l.Tags,
		}
	}
	return &scrapperpb.ListLinksResponse{Links: pbLinks}, nil
}
