package cocoonset

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

func TestApplyUnsuspendClearsHibernateAnnotation(t *testing.T) {
	scheme := testScheme(t)

	mainPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"},
	}
	(meta.HibernateState(true)).Apply(mainPod)

	subPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "ns"},
	}
	(meta.HibernateState(true)).Apply(subPod)

	tbPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-tb", Namespace: "ns"},
	}
	// tbPod was never suspended; must be skipped.

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(mainPod, subPod, tbPod).
		Build()

	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      mainPod,
		sub:       map[int32]*corev1.Pod{1: subPod},
		toolbox:   map[string]*corev1.Pod{"tb": tbPod},
		allByName: map[string]*corev1.Pod{"demo-0": mainPod, "demo-1": subPod, "demo-tb": tbPod},
	}

	if err := r.applyUnsuspend(t.Context(), "ns", classified); err != nil {
		t.Fatalf("applyUnsuspend: %v", err)
	}

	for _, tc := range []struct {
		name        string
		wantCleared bool
	}{
		{"demo-0", true},
		{"demo-1", true},
		{"demo-tb", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got corev1.Pod
			if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: tc.name}, &got); err != nil {
				t.Fatalf("get %s: %v", tc.name, err)
			}
			hibernated := bool(meta.ReadHibernateState(&got))
			if tc.wantCleared && hibernated {
				t.Errorf("%s: hibernate annotation should be cleared", tc.name)
			}
			if !tc.wantCleared && hibernated {
				t.Errorf("%s: hibernate annotation unexpectedly set", tc.name)
			}
		})
	}
}

func TestApplyUnsuspendNoopOnCleanSet(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	mainPod := buildAgentPod(cs, 0, "", "", scheme)
	subPod := buildAgentPod(cs, 1, "vk-ns-demo-0", "", scheme)

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(mainPod, subPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      mainPod,
		sub:       map[int32]*corev1.Pod{1: subPod},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{mainPod.Name: mainPod, subPod.Name: subPod},
	}

	if err := r.applyUnsuspend(t.Context(), "default", classified); err != nil {
		t.Errorf("applyUnsuspend on clean set: %v", err)
	}
}

// TestApplyUnsuspendSkipsPodHibernatedByCR ensures pods targeted by an active
// Hibernate CR are not cleared, avoiding a race with the hibernation reconciler.
func TestApplyUnsuspendSkipsPodHibernatedByCR(t *testing.T) {
	scheme := testScheme(t)

	hibernated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"},
	}
	(meta.HibernateState(true)).Apply(hibernated)

	// Also hibernated but not named in any CR -- proves skip is selective.
	leftover := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "ns"},
	}
	(meta.HibernateState(true)).Apply(leftover)

	hibCR := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-hib", Namespace: "ns"},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireHibernate,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
	}

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hibernated, leftover, hibCR).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      hibernated,
		sub:       map[int32]*corev1.Pod{1: leftover},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{"demo-0": hibernated, "demo-1": leftover},
	}

	if err := r.applyUnsuspend(t.Context(), "ns", classified); err != nil {
		t.Fatalf("applyUnsuspend: %v", err)
	}

	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("get demo-0: %v", err)
	}
	if !bool(meta.ReadHibernateState(&got)) {
		t.Errorf("demo-0 was hibernated by CR; applyUnsuspend must leave it set")
	}

	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-1"}, &got); err != nil {
		t.Fatalf("get demo-1: %v", err)
	}
	if bool(meta.ReadHibernateState(&got)) {
		t.Errorf("demo-1 had no CR; applyUnsuspend must clear it")
	}
}
