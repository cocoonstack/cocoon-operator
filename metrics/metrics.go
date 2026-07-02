// Package metrics defines the prometheus collectors for cocoon-operator.
package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	metricNamespace = "cocoon"
	metricSubsystem = "operator"
)

var (
	SubAgentRebuildTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Subsystem: metricSubsystem,
			Name:      "subagent_rebuild_total",
			Help:      "Number of sub-agent rebuilds triggered by triageSubAgent.",
		},
		[]string{"namespace", "cocoonset"},
	)

	SubAgentDeadLetterTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Subsystem: metricSubsystem,
			Name:      "subagent_dead_letter_total",
			Help:      "Number of sub-agents marked dead-letter after exhausting rebuild attempts.",
		},
		[]string{"namespace", "cocoonset"},
	)

	HibernatePhaseDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Subsystem: metricSubsystem,
			Name:      "hibernate_phase_duration_seconds",
			Help:      "Time spent in CocoonHibernation Hibernating phase, bucketed by result.",
			Buckets:   []float64{10, 30, 60, 180, 600, 1800},
		},
		[]string{"result"}, // result=ok|timeout
	)

	WakePhaseDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Subsystem: metricSubsystem,
			Name:      "wake_phase_duration_seconds",
			Help:      "Time spent in CocoonHibernation Waking phase, bucketed by result.",
			Buckets:   []float64{10, 30, 60, 180, 600, 1800},
		},
		[]string{"result"}, // result=ok|timeout
	)

	LifecycleStateFailedObservedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Subsystem: metricSubsystem,
			Name:      "lifecycle_state_failed_observed_total",
			Help:      "Number of times the operator consumed a Pod lifecycle-state=Failed annotation.",
		},
		[]string{"phase"},
	)
)

// Register installs all operator collectors into reg (controller-runtime's
// registry in production) so they surface on the /metrics endpoint.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(
		SubAgentRebuildTotal,
		SubAgentDeadLetterTotal,
		HibernatePhaseDurationSeconds,
		WakePhaseDurationSeconds,
		LifecycleStateFailedObservedTotal,
	)
}
