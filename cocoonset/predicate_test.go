package cocoonset

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
)

func TestSetRelevantChangeAdmitsGenerationAndStatusChangesOnly(t *testing.T) {
	base := &cocoonv1.CocoonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns", Generation: 4},
		Status:     cocoonv1.CocoonSetStatus{Phase: cocoonv1.CocoonSetPhaseRunning, ReadyAgents: 2},
	}
	for _, tc := range []struct {
		name   string
		mutate func(*cocoonv1.CocoonSet)
		want   bool
	}{
		{name: "spec edit", mutate: func(cs *cocoonv1.CocoonSet) { cs.Generation++ }, want: true},
		{name: "status written from a stale view", mutate: func(cs *cocoonv1.CocoonSet) {
			cs.Status.Phase, cs.Status.ReadyAgents = cocoonv1.CocoonSetPhaseScaling, 1
		}, want: true},
		{name: "annotation patch", mutate: func(cs *cocoonv1.CocoonSet) {
			cs.Annotations = map[string]string{annotationHibernateReclaim: `{"generation":4}`}
		}, want: false},
		{name: "resync", mutate: func(*cocoonv1.CocoonSet) {}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			updated := base.DeepCopy()
			tc.mutate(updated)
			if got := setRelevantChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: updated}); got != tc.want {
				t.Fatalf("Update = %v, want %v", got, tc.want)
			}
		})
	}
}
