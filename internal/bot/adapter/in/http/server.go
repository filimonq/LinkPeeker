package bothttp

import (
	"context"
	"net/http"
	"time"
)

// Server is the HTTP server for bot.
type Server struct {
	srv *http.Server
}

// NewServer creates a new Server.
func NewServer(addr string, h *Handler) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/updates", h.handleUpdates)
	const readHeaderTimeout = 5 * time.Second

	return &Server{
		srv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: readHeaderTimeout,
		},
	}
}

// Start begins listening for incoming requests.
func (s *Server) Start() error {
	return s.srv.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// Handler returns the underlying http.Handler for testing.
func (s *Server) Handler() http.Handler {
	return s.srv.Handler
}
