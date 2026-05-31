package scrapperhttp

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is an HTTP server for scrapper.
type Server struct {
	server *http.Server
}

func NewServer(addr string, handler *Handler, registry *prometheus.Registry, middlewares ...func(http.Handler) http.Handler) *Server {
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	if registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	}

	var h http.Handler = mux
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}

	const readHeaderTimeout = 10 * time.Second
	return &Server{
		server: &http.Server{
			Addr:              addr,
			Handler:           h,
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
	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("http server shutdown: %w", err)
	}
	return nil
}

// Handler returns the underlying http.Handler for testing.
func (s *Server) Handler() http.Handler {
	return s.server.Handler
}
