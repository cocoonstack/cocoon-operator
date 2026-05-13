// Package metrics defines the prometheus collectors for cocoon-operator.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const metricNamespace = "cocoon_operator"

var (
	SubAgentRebuildTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "subagent_rebuild_total",
			Help:      "Number of sub-agent rebuilds triggered by triageSubAgent.",
		},
		[]string{"namespace", "cocoonset"},
	)

	SubAgentDeadLetterTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "subagent_dead_letter_total",
			Help:      "Number of sub-agents marked dead-letter after exhausting rebuild attempts.",
		},
		[]string{"namespace", "cocoonset"},
	)

	HibernatePhaseDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "hibernate_phase_duration_seconds",
			Help:      "Time spent in CocoonHibernation Hibernating phase, bucketed by result.",
			Buckets:   []float64{10, 30, 60, 180, 600, 1800},
		},
		[]string{"result"}, // result=ok|failed|timeout
	)

	WakePhaseDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "wake_phase_duration_seconds",
			Help:      "Time spent in CocoonHibernation Waking phase, bucketed by result.",
			Buckets:   []float64{10, 30, 60, 180, 600, 1800},
		},
		[]string{"result"},
	)

	LifecycleStateFailedObservedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "lifecycle_state_failed_observed_total",
			Help:      "Number of times the operator consumed a Pod lifecycle-state=Failed annotation.",
		},
		[]string{"phase"},
	)
)

// Register installs all collectors into controller-runtime's registry so they
// surface on the existing /metrics endpoint.
func Register() {
	ctrlmetrics.Registry.MustRegister(
		SubAgentRebuildTotal,
		SubAgentDeadLetterTotal,
		HibernatePhaseDurationSeconds,
		WakePhaseDurationSeconds,
		LifecycleStateFailedObservedTotal,
	)
}
