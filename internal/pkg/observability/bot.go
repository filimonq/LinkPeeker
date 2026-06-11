package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type BotMetrics struct {
	Registry          *prometheus.Registry
	http              *httpMetrics
	CommandRequests   *prometheus.CounterVec
	CommandDuration   *prometheus.HistogramVec
	SentNotifications prometheus.Counter
}

func NewBotMetrics() *BotMetrics {
	reg := newRegistry()
	httpMetrics := newHTTPMetrics(reg)

	commandRequests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "command_requests_total",
			Help: "Total number of handled bot commands.",
		},
		[]string{"command"},
	)
	commandDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "command_duration_ms_total",
			Help:    "Duration of bot command-related operations in milliseconds.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		},
		[]string{"scope", "scope_type"},
	)
	sentNotifications := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "sent_notification_total",
			Help: "Total number of sent link update notifications.",
		},
	)

	reg.MustRegister(commandRequests, commandDuration, sentNotifications)

	return &BotMetrics{
		Registry:          reg,
		http:              httpMetrics,
		CommandRequests:   commandRequests,
		CommandDuration:   commandDuration,
		SentNotifications: sentNotifications,
	}
}

func (m *BotMetrics) HTTPMiddleware() func(http.Handler) http.Handler {
	return m.http.Middleware(nil)
}

func (m *BotMetrics) ObserveCommandRequest(command string) {
	m.CommandRequests.WithLabelValues(command).Inc()
}

func (m *BotMetrics) ObserveCommandDuration(scope, scopeType string, d time.Duration) {
	m.CommandDuration.WithLabelValues(scope, scopeType).Observe(durationMillis(d))
}

func (m *BotMetrics) IncSentNotification() {
	m.SentNotifications.Inc()
}
