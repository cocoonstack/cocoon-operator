package cocoonset

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

func TestMarkRestoreIfHibernated(t *testing.T) {
	cases := []struct {
		name     string
		intent   bool
		present  map[string]bool
		probeErr error
		wantSet  bool
		wantErr  bool
	}{
		{"no intent skips the probe", false, map[string]bool{"vm:hibernate": true}, nil, false, false},
		{"intent and snapshot present", true, map[string]bool{"vm:hibernate": true}, nil, true, false},
		{"intent but no snapshot", true, map[string]bool{}, nil, false, false},
		{"probe error fails closed", true, nil, errors.New("boom"), false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &Reconciler{Registry: &fakeRegistry{present: c.present, probeErr: c.probeErr}}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "p", Namespace: "ns",
				Annotations: map[string]string{meta.AnnotationVMName: "vm"},
			}}
			err := r.markRestoreIfHibernated(t.Context(), pod, c.intent)
			if (err != nil) != c.wantErr {
				t.Fatalf("markRestoreIfHibernated err = %v, wantErr %v", err, c.wantErr)
			}
			if got := meta.ReadRestoreFromHibernate(pod); got != c.wantSet {
				t.Errorf("restore-from-hibernate set = %v, want %v", got, c.wantSet)
			}
		})
	}
}

func TestMarkRestoreIfHibernatedNoRegistry(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{meta.AnnotationVMName: "vm"},
	}}
	r := &Reconciler{} // Registry nil (OCI_REGISTRY unset deployment)
	if err := r.markRestoreIfHibernated(t.Context(), pod, true); err != nil {
		t.Fatalf("nil registry should be a no-op, got %v", err)
	}
	if meta.ReadRestoreFromHibernate(pod) {
		t.Error("no registry must not flag restore")
	}
}

func TestRestorableFromHibernateByCR(t *testing.T) {
	hib := func(pod string, phase cocoonv1.CocoonHibernationPhase) *cocoonv1.CocoonHibernation {
		return &cocoonv1.CocoonHibernation{
			ObjectMeta: metav1.ObjectMeta{Name: "h-" + pod, Namespace: "ns"},
			Spec: cocoonv1.CocoonHibernationSpec{
				PodRef: cocoonv1.HibernationPodRef{Name: pod},
				Desire: cocoonv1.HibernationDesireHibernate,
			},
			Status: cocoonv1.CocoonHibernationStatus{Phase: phase},
		}
	}
	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(
		hib("hibernated", cocoonv1.CocoonHibernationPhaseHibernated),
		hib("waking", cocoonv1.CocoonHibernationPhaseWaking),
		hib("active", cocoonv1.CocoonHibernationPhaseActive),
		hib("hibernating", cocoonv1.CocoonHibernationPhaseHibernating),
	).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	got, err := r.restorableFromHibernateByCR(t.Context(), "ns")
	if err != nil {
		t.Fatalf("restorableFromHibernateByCR: %v", err)
	}
	for _, want := range []string{"hibernated", "waking"} {
		if _, ok := got[want]; !ok {
			t.Errorf("pod %q should be restorable", want)
		}
	}
	for _, no := range []string{"active", "hibernating"} {
		if _, ok := got[no]; ok {
			t.Errorf("pod %q must not be restorable (phase excludes it)", no)
		}
	}
}
