package bothttp

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Server is the HTTP server for bot.
type Server struct {
	srv *http.Server
}

func NewServer(addr string, h *Handler, middlewares ...func(http.Handler) http.Handler) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/updates", h.handleUpdates)

	var handler http.Handler = mux
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}

	const readHeaderTimeout = 5 * time.Second

	return &Server{
		srv: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: readHeaderTimeout,
		},
	}
}

// Start begins listening for incoming requests.
func (s *Server) Start() error {
	err := s.srv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server listen and serve: %w", err)
	}
	return nil
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)
	if err != nil {
		return fmt.Errorf("http server shutdown: %w", err)
	}
	return nil
}

// Handler returns the underlying http.Handler for testing.
func (s *Server) Handler() http.Handler {
	return s.srv.Handler
}
