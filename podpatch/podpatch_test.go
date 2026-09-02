package podpatch

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cocoonstack/cocoon-common/meta"
)

func TestHibernateStateShortCircuitsNoOp(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
	}
	meta.HibernateState(true).Apply(pod)
	cli := newFakeClient(t, pod.DeepCopy())

	if err := HibernateState(t.Context(), cli, pod, true); err != nil {
		t.Fatalf("no-op HibernateState must not reach the client: %v", err)
	}
}

func TestHibernateStateSetsAnnotation(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}}
	cli := newFakeClient(t, pod.DeepCopy())

	if err := HibernateState(t.Context(), cli, pod, true); err != nil {
		t.Fatalf("HibernateState: %v", err)
	}

	var got corev1.Pod
	if err := cli.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "demo"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bool(meta.ReadHibernateState(&got)) {
		t.Errorf("hibernate annotation not persisted: %v", got.Annotations)
	}
}

func TestHibernateStateClearsAnnotation(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}}
	meta.HibernateState(true).Apply(pod)
	cli := newFakeClient(t, pod.DeepCopy())

	if err := HibernateState(t.Context(), cli, pod, false); err != nil {
		t.Fatalf("HibernateState(false): %v", err)
	}

	var got corev1.Pod
	if err := cli.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "demo"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := got.Annotations[meta.AnnotationHibernate]; ok {
		t.Errorf("hibernate annotation should be cleared, got %v", got.Annotations)
	}
}

func TestCocoonSetGenerationWritesValue(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}}
	cli := newFakeClient(t, pod.DeepCopy())

	if err := CocoonSetGeneration(t.Context(), cli, pod, 42); err != nil {
		t.Fatalf("PatchCocoonSetGeneration: %v", err)
	}

	var got corev1.Pod
	if err := cli.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "demo"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Annotations[meta.AnnotationCocoonSetGeneration] != "42" {
		t.Errorf("annotation = %q, want 42", got.Annotations[meta.AnnotationCocoonSetGeneration])
	}
}

func TestCocoonSetGenerationShortCircuitsNoOp(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "demo", Namespace: "ns",
		Annotations: map[string]string{meta.AnnotationCocoonSetGeneration: "7"},
	}}
	cli := newFakeClient(t, pod.DeepCopy())

	if err := CocoonSetGeneration(t.Context(), cli, pod, 7); err != nil {
		t.Fatalf("no-op PatchCocoonSetGeneration must not reach the client: %v", err)
	}
}

func TestKeepSnapshotOnDeletePersistsAndShortCircuits(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}}
	cli := newFakeClient(t, pod.DeepCopy())

	if err := KeepSnapshotOnDelete(t.Context(), cli, pod); err != nil {
		t.Fatalf("PatchKeepSnapshotOnDelete: %v", err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "demo"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !meta.ReadKeepSnapshotOnDelete(&got) {
		t.Errorf("flag must reach the API server before the delete lands: %v", got.Annotations)
	}
	if err := KeepSnapshotOnDelete(t.Context(), cli, &got); err != nil {
		t.Fatalf("re-flagging an already-flagged pod must not reach the client: %v", err)
	}
}

func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	return ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}
