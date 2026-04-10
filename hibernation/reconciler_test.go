package hibernation

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// fakeRegistry is a SnapshotRegistry stand-in for the hibernation
// tests. The behaviour of HasManifest / DeleteManifest is fully
// driven by the bool fields so each test can simulate a particular
// epoch state without standing up a real client.
type fakeRegistry struct {
	manifestPresent bool
	manifestErr     error
	deleteCalled    bool
	deleteErr       error
}

func (f *fakeRegistry) HasManifest(_ context.Context, _, _ string) (bool, error) {
	if f.manifestErr != nil {
		return false, f.manifestErr
	}
	return f.manifestPresent, nil
}

func (f *fakeRegistry) DeleteManifest(_ context.Context, _, _ string) error {
	f.deleteCalled = true
	return f.deleteErr
}

func TestReadyConditionMaps(t *testing.T) {
	cases := []struct {
		phase  cocoonv1.CocoonHibernationPhase
		status metav1.ConditionStatus
	}{
		{cocoonv1.CocoonHibernationPhaseHibernated, metav1.ConditionTrue},
		{cocoonv1.CocoonHibernationPhaseActive, metav1.ConditionTrue},
		{cocoonv1.CocoonHibernationPhaseFailed, metav1.ConditionFalse},
		{cocoonv1.CocoonHibernationPhaseHibernating, metav1.ConditionFalse},
		{cocoonv1.CocoonHibernationPhaseWaking, metav1.ConditionFalse},
		{cocoonv1.CocoonHibernationPhasePending, metav1.ConditionFalse},
	}
	for _, c := range cases {
		t.Run(string(c.phase), func(t *testing.T) {
			cond := readyCondition(c.phase, 1)
			if cond.Status != c.status {
				t.Errorf("phase %q -> status %q, want %q", c.phase, cond.Status, c.status)
			}
			if cond.ObservedGeneration != 1 {
				t.Errorf("observedGeneration: %d", cond.ObservedGeneration)
			}
		})
	}
}

func TestIsContainerRunning(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	if !isContainerRunning(pod) {
		t.Errorf("expected running")
	}

	waiting := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}},
			}},
		},
	}
	if isContainerRunning(waiting) {
		t.Errorf("expected not running")
	}
}

func TestFakeRegistryGetMissing(t *testing.T) {
	r := &fakeRegistry{}
	present, err := r.HasManifest(t.Context(), "x", "hibernate")
	if err != nil {
		t.Fatalf("has: %v", err)
	}
	if present {
		t.Errorf("expected manifest absent")
	}
}

func TestFakeRegistryGetPresent(t *testing.T) {
	r := &fakeRegistry{manifestPresent: true}
	present, err := r.HasManifest(t.Context(), "x", "hibernate")
	if err != nil {
		t.Fatalf("has: %v", err)
	}
	if !present {
		t.Errorf("expected manifest present")
	}
}

func TestFakeRegistryGetErrorPropagates(t *testing.T) {
	r := &fakeRegistry{manifestErr: errors.New("transport boom")}
	if _, err := r.HasManifest(t.Context(), "x", "hibernate"); err == nil {
		t.Errorf("expected transport error to surface")
	}
}

func TestFakeRegistryDeleteRecords(t *testing.T) {
	r := &fakeRegistry{}
	if err := r.DeleteManifest(t.Context(), "x", "hibernate"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !r.deleteCalled {
		t.Errorf("delete was not recorded")
	}
}

// TestPodVMNameRoundtrip exercises the meta.VMSpec annotation
// roundtrip on the same shape of pod the cocoonset reconciler
// produces. Building it inline (rather than reaching across into
// the cocoonset package) keeps the hibernation tests free of any
// reverse dependency on cocoonset.
func TestPodVMNameRoundtrip(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	(&meta.VMSpec{VMName: "vk-ns-demo-0", Managed: true}).Apply(pod)
	if got := meta.ParseVMSpec(pod).VMName; got != "vk-ns-demo-0" {
		t.Errorf("vmName roundtrip: %q", got)
	}
}
