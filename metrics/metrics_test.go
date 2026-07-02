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

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	if !slices.Contains(names, "cocoon_operator_subagent_rebuild_total") {
		t.Errorf("want metric cocoon_operator_subagent_rebuild_total, got %v", names)
	}
}
