package observability

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsReadHeaderTimeout = 5 * time.Second

type MetricsServer struct {
	server *http.Server
}

func NewMetricsServer(addr string, registry *prometheus.Registry) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	return &MetricsServer{
		server: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: metricsReadHeaderTimeout,
		},
	}
}

func (s *MetricsServer) Start() error {
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("metrics server listen and serve: %w", err)
	}
	return nil
}

func (s *MetricsServer) Shutdown(ctx context.Context) error {
	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("metrics server shutdown: %w", err)
	}
	return nil
}
