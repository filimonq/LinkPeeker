package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type ScrapperMetrics struct {
	Registry        *prometheus.Registry
	http            *httpMetrics
	APIRequests     *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	LinksOnTrack    *prometheus.GaugeVec
}

func NewScrapperMetrics() *ScrapperMetrics {
	reg := newRegistry()
	httpMetrics := newHTTPMetrics(reg)

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

	reg.MustRegister(apiRequests, requestDuration, linksOnTrack)

	return &ScrapperMetrics{
		Registry:        reg,
		http:            httpMetrics,
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
