package observability

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

type ScrapperMetrics struct {
	Registry        *prometheus.Registry
	http            *httpMetrics
	grpcRequests    *prometheus.CounterVec
	grpcDuration    *prometheus.HistogramVec
	APIRequests     *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	LinksOnTrack    *prometheus.GaugeVec
}

func NewScrapperMetrics() *ScrapperMetrics {
	reg := newRegistry()
	httpMetrics := newHTTPMetrics(reg)
	grpcRequests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_server_requests_total",
			Help: "Total number of handled gRPC requests.",
		},
		[]string{"method", "status"},
	)
	grpcDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_server_request_duration_seconds",
			Help:    "gRPC request latency in seconds.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"method"},
	)

	apiRequests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "api_requests_total",
			Help: "Total number of incoming scrapper API requests.",
		},
		[]string{"source"},
	)
	requestDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "request_duration_ms_total",
			Help:    "Duration of scrapper external and internal operations in milliseconds.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000},
		},
		[]string{"scope", "scope_type"},
	)
	linksOnTrack := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "links_on_track_total",
			Help: "Number of tracked links by source domain.",
		},
		[]string{"tracked_source"},
	)

	reg.MustRegister(grpcRequests, grpcDuration, apiRequests, requestDuration, linksOnTrack)

	return &ScrapperMetrics{
		Registry:        reg,
		http:            httpMetrics,
		grpcRequests:    grpcRequests,
		grpcDuration:    grpcDuration,
		APIRequests:     apiRequests,
		RequestDuration: requestDuration,
		LinksOnTrack:    linksOnTrack,
	}
}

func (m *ScrapperMetrics) HTTPMiddleware() func(http.Handler) http.Handler {
	return m.http.Middleware(func(r *http.Request) {
		route := requestRoute(r)
		if route == "/metrics" {
			return
		}
		m.APIRequests.WithLabelValues(route).Inc()
	})
}

func (m *ScrapperMetrics) ObserveRequestDuration(scope, scopeType string, d time.Duration) {
	m.RequestDuration.WithLabelValues(scope, scopeType).Observe(durationMillis(d))
}

func (m *ScrapperMetrics) SetTrackedLinks(source string, value float64) {
	m.LinksOnTrack.WithLabelValues(source).Set(value)
}

func (m *ScrapperMetrics) UnaryServerInterceptor() googlegrpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *googlegrpc.UnaryServerInfo,
		handler googlegrpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		statusCode := grpcstatus.Code(err)
		m.grpcRequests.WithLabelValues(info.FullMethod, statusCode.String()).Inc()
		m.grpcDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

func (m *ScrapperMetrics) ObserveGRPCError(method string, err error) {
	statusCode := codes.OK
	if err != nil {
		statusCode = grpcstatus.Code(err)
	}
	m.grpcRequests.WithLabelValues(method, statusCode.String()).Inc()
}
