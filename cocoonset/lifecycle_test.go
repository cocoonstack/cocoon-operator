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

func TestSyncCocoonSetGenerationStampsAllOwnedPods(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) { cs.Generation = 7 })

	mainPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	subPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "demo-1", Namespace: "ns",
		Annotations: map[string]string{meta.AnnotationCocoonSetGeneration: "5"},
	}}

	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(mainPod, subPod).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      mainPod,
		sub:       map[int32]*corev1.Pod{1: subPod},
		allByName: map[string]*corev1.Pod{"demo-0": mainPod, "demo-1": subPod},
	}

	if err := r.syncCocoonSetGeneration(t.Context(), cs, classified); err != nil {
		t.Fatalf("syncCocoonSetGeneration: %v", err)
	}

	for _, name := range []string{"demo-0", "demo-1"} {
		t.Run(name, func(t *testing.T) {
			var got corev1.Pod
			if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: name}, &got); err != nil {
				t.Fatalf("get %s: %v", name, err)
			}
			if got.Annotations[meta.AnnotationCocoonSetGeneration] != "7" {
				t.Errorf("%s generation annotation = %q, want 7",
					name, got.Annotations[meta.AnnotationCocoonSetGeneration])
			}
		})
	}
}

func TestSyncCocoonSetGenerationNoOpWhenAlreadyCurrent(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) { cs.Generation = 3 })

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "demo-0", Namespace: "ns",
		Annotations: map[string]string{meta.AnnotationCocoonSetGeneration: "3"},
	}}
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      pod,
		allByName: map[string]*corev1.Pod{"demo-0": pod},
	}

	// PatchCocoonSetGeneration must short-circuit on equal — the fake
	// client would error on a Patch with empty body otherwise.
	if err := r.syncCocoonSetGeneration(t.Context(), cs, classified); err != nil {
		t.Fatalf("syncCocoonSetGeneration: %v", err)
	}
}
