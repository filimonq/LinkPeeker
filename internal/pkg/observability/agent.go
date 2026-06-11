package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type AgentMetrics struct {
	Registry       *prometheus.Registry
	PipelineEvents *prometheus.CounterVec
	OperationTime  *prometheus.HistogramVec
}

func NewAgentMetrics() *AgentMetrics {
	reg := newRegistry()

	pipelineEvents := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agent_pipeline_events_total",
			Help: "Total number of AI agent pipeline events by stage and result.",
		},
		[]string{"stage", "result"},
	)
	operationTime := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agent_operation_duration_ms_total",
			Help:    "Duration of AI agent operations in milliseconds.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000},
		},
		[]string{"scope", "scope_type"},
	)

	reg.MustRegister(pipelineEvents, operationTime)

	return &AgentMetrics{
		Registry:       reg,
		PipelineEvents: pipelineEvents,
		OperationTime:  operationTime,
	}
}

func (m *AgentMetrics) ObservePipelineEvent(stage, result string) {
	m.PipelineEvents.WithLabelValues(stage, result).Inc()
}

func (m *AgentMetrics) ObserveOperationDuration(scope, scopeType string, d time.Duration) {
	m.OperationTime.WithLabelValues(scope, scopeType).Observe(durationMillis(d))
}
