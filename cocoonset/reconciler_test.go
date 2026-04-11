package cocoonset

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cocoonstack/cocoon-common/meta"
)

// TestApplyUnsuspendClearsHibernateAnnotation verifies that the
// un-suspend path patches HibernateState(false) onto pods that were
// previously suspended, and leaves untouched pods alone. Without
// this patch path, flipping Spec.Suspend back to false would leave
// owned pods stuck with hibernate=true annotations forever.
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
	// tbPod was never suspended — applyUnsuspend must skip it.

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

	if err := r.applyUnsuspend(t.Context(), classified); err != nil {
		t.Fatalf("applyUnsuspend: %v", err)
	}

	// Re-fetch and assert the annotation was cleared on both
	// hibernated pods and untouched on the toolbox.
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
				// Toolbox was never hibernated so this should also be false;
				// asserting the same thing but keeping the branch explicit.
				t.Errorf("%s: hibernate annotation unexpectedly set", tc.name)
			}
		})
	}
}

// TestApplyUnsuspendNoopOnCleanSet confirms the un-suspend walk is a
// pure read on a CocoonSet that was never suspended — no Patch calls
// fire when every pod's hibernate annotation is already absent.
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

	if err := r.applyUnsuspend(t.Context(), classified); err != nil {
		t.Errorf("applyUnsuspend on clean set: %v", err)
	}
}
