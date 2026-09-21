package podpatch

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cocoonstack/cocoon-common/meta"
)

func TestHibernateStateSetsAnnotation(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}}
	cli := newFakeClientBuilder(t).WithObjects(pod.DeepCopy()).Build()

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
	cli := newFakeClientBuilder(t).WithObjects(pod.DeepCopy()).Build()

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
	cli := newFakeClientBuilder(t).WithObjects(pod.DeepCopy()).Build()

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

func TestKeepSnapshotOnDeletePersists(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}}
	cli := newFakeClientBuilder(t).WithObjects(pod.DeepCopy()).Build()

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
}

func TestDesiredStateSkipsTheClient(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "demo", Namespace: "ns",
		Annotations: map[string]string{meta.AnnotationCocoonSetGeneration: "7"},
	}}
	meta.HibernateState(true).Apply(pod)
	meta.MarkKeepSnapshotOnDelete(pod)
	clean := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "clean", Namespace: "ns"}}
	patches := 0
	cli := newFakeClientBuilder(t).WithObjects(pod.DeepCopy(), clean.DeepCopy()).WithInterceptorFuncs(interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patches++
			return c.Patch(ctx, obj, patch, opts...)
		},
	}).Build()

	if err := HibernateState(t.Context(), cli, pod, true); err != nil {
		t.Fatalf("HibernateState: %v", err)
	}
	if err := KeepSnapshotOnDelete(t.Context(), cli, pod); err != nil {
		t.Fatalf("KeepSnapshotOnDelete: %v", err)
	}
	if err := CocoonSetGeneration(t.Context(), cli, pod, 7); err != nil {
		t.Fatalf("CocoonSetGeneration: %v", err)
	}
	if err := HibernateState(t.Context(), cli, clean, false); err != nil {
		t.Fatalf("HibernateState(false) on a clean pod: %v", err)
	}
	if patches != 0 {
		t.Errorf("no-op patches reached the client: %d", patches)
	}
}

func newFakeClientBuilder(t *testing.T) *ctrlfake.ClientBuilder {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	return ctrlfake.NewClientBuilder().WithScheme(scheme)
}
