package main

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cocoonv1alpha1 "github.com/cocoonstack/cocoon-common/apis/v1alpha1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// fakeRegistry is a SnapshotRegistry stand-in for the hibernation
// tests. The behaviour of GetManifest / DeleteManifest is fully
// driven by the bool fields so each test can simulate a particular
// epoch state without standing up a real client.
type fakeRegistry struct {
	manifestPresent bool
	manifestErr     error
	deleteCalled    bool
	deleteErr       error
}

func (f *fakeRegistry) GetManifest(_ context.Context, _, _ string) ([]byte, string, error) {
	if f.manifestErr != nil {
		return nil, "", f.manifestErr
	}
	if f.manifestPresent {
		return []byte("{}"), "application/json", nil
	}
	return nil, "", errors.New("not found")
}

func (f *fakeRegistry) DeleteManifest(_ context.Context, _, _ string) error {
	f.deleteCalled = true
	return f.deleteErr
}

func TestReadyConditionMaps(t *testing.T) {
	cases := []struct {
		phase  cocoonv1alpha1.CocoonHibernationPhase
		status metav1.ConditionStatus
	}{
		{cocoonv1alpha1.CocoonHibernationPhaseHibernated, metav1.ConditionTrue},
		{cocoonv1alpha1.CocoonHibernationPhaseActive, metav1.ConditionTrue},
		{cocoonv1alpha1.CocoonHibernationPhaseFailed, metav1.ConditionFalse},
		{cocoonv1alpha1.CocoonHibernationPhaseHibernating, metav1.ConditionFalse},
		{cocoonv1alpha1.CocoonHibernationPhaseWaking, metav1.ConditionFalse},
		{cocoonv1alpha1.CocoonHibernationPhasePending, metav1.ConditionFalse},
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
	_, _, err := r.GetManifest(context.Background(), "x", "hibernate")
	if err == nil {
		t.Errorf("expected error when manifest is not present")
	}
}

func TestFakeRegistryGetPresent(t *testing.T) {
	r := &fakeRegistry{manifestPresent: true}
	body, _, err := r.GetManifest(context.Background(), "x", "hibernate")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(body) == 0 {
		t.Errorf("body should not be empty when manifest present")
	}
}

func TestFakeRegistryDeleteRecords(t *testing.T) {
	r := &fakeRegistry{}
	if err := r.DeleteManifest(context.Background(), "x", "hibernate"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !r.deleteCalled {
		t.Errorf("delete was not recorded")
	}
}

func TestPodVMNameRoundtrip(t *testing.T) {
	cs := newCocoonSet("demo")
	pod := buildAgentPod(cs, 0, "")
	got := meta.ParseVMSpec(pod).VMName
	if got != "vk-ns-demo-0" {
		t.Errorf("vmName roundtrip: %q", got)
	}
}
