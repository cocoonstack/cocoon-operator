package metrics

import (
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// Emitted names are the operator's monitoring contract: the Namespace/Subsystem
// split must still render the flat cocoon_operator_ prefix, byte-for-byte.
func TestRegisterEmitsStableNames(t *testing.T) {
	reg := prometheus.NewRegistry()
	Register(reg)
	SubAgentRebuildTotal.WithLabelValues("ns", "cs").Inc()
	SubAgentDeadLetterTotal.WithLabelValues("ns", "cs").Inc()
	HibernatePhaseDurationSeconds.WithLabelValues("ok").Observe(1)
	WakePhaseDurationSeconds.WithLabelValues("ok").Observe(1)
	LifecycleStateFailedObservedTotal.WithLabelValues("Pending").Inc()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	for _, want := range []string{
		"cocoon_operator_subagent_rebuild_total",
		"cocoon_operator_subagent_dead_letter_total",
		"cocoon_operator_hibernate_phase_duration_seconds",
		"cocoon_operator_wake_phase_duration_seconds",
		"cocoon_operator_lifecycle_state_failed_observed_total",
	} {
		if !slices.Contains(names, want) {
			t.Errorf("want metric %s, got %v", want, names)
		}
	}
}
