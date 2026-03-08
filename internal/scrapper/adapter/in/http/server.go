package scrapperhttp

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Server is an HTTP server for scrapper.
type Server struct {
	server *http.Server
}

// NewServer creates a new Server.
func NewServer(addr string, handler *Handler) *Server {
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	const readHeaderTimeout = 10 * time.Second
	return &Server{
		server: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: readHeaderTimeout,
		},
	}
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen and serve: %w", err)
	}
	return nil
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) Handler() http.Handler {
	return s.server.Handler
}
