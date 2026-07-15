package cocoonset

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// TestReconcileSteadyStateSkipsHibernationList pins the lazy load: with every
// desired pod already present, the reconcile must not pay the namespace-wide
// CocoonHibernation List.
func TestReconcileSteadyStateSkipsHibernationList(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
		cs.Spec.Agent.Replicas = 1
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "tb", Image: "img", Mode: cocoonv1.ToolboxModeRun},
		}
	})
	mainPod := readyPod(mustBuildAgentPod(t, cs, 0, "", "", scheme))
	subPod := readyPod(mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0", "", scheme))
	tbPod := readyPod(mustBuildToolboxPod(t, cs, cs.Spec.Toolboxes[0], scheme))

	var lists atomic.Int32
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, mainPod, subPod, tbPod).
		WithStatusSubresource(&cocoonv1.CocoonSet{}).
		WithInterceptorFuncs(countHibernationLists(&lists)).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Guards the assertion below against a vacuous pass: 0 Lists would also
	// follow from an early return or from triage deleting a drifted pod.
	for _, p := range []*corev1.Pod{mainPod, subPod, tbPod} {
		if err := cli.Get(t.Context(), client.ObjectKeyFromObject(p), &corev1.Pod{}); err != nil {
			t.Fatalf("pod %s must survive a steady reconcile: %v", p.Name, err)
		}
	}
	if got := lists.Load(); got != 0 {
		t.Errorf("steady reconcile listed CocoonHibernations %d times, want 0", got)
	}
}

// TestReconcileMissingPodsListsHibernationsOnce pins the sharing: a reconcile
// missing both a sub-agent and a toolbox loads restore intent exactly once.
func TestReconcileMissingPodsListsHibernationsOnce(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
		cs.Spec.Agent.Replicas = 2
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "tb", Image: "img", Mode: cocoonv1.ToolboxModeRun},
		}
	})
	mainPod := readyPod(mustBuildAgentPod(t, cs, 0, "", "", scheme))

	var lists atomic.Int32
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, mainPod).
		WithStatusSubresource(&cocoonv1.CocoonSet{}).
		WithInterceptorFuncs(countHibernationLists(&lists)).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := lists.Load(); got != 1 {
		t.Errorf("reconcile listed CocoonHibernations %d times, want exactly 1", got)
	}
	for _, name := range []string{"demo-1", "demo-2", toolboxPodName(cs.Name, "tb")} {
		if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: name}, &corev1.Pod{}); err != nil {
			t.Errorf("pod %s should have been created: %v", name, err)
		}
	}
}

// TestReconcileUnrelatedKeyProgressesWhileProbeBlocks pins what concurrency
// buys: with one CocoonSet's registry probe wedged, a second must still finish.
// At MaxConcurrentReconciles=1 the worker pool would serialize them.
func TestReconcileUnrelatedKeyProgressesWhileProbeBlocks(t *testing.T) {
	scheme := testScheme(t)
	blocked, wedged := newCocoonSet("blocked", withMainRestore), newCocoonSet("free", withMainRestore)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(blocked, wedged, hibernatedFor(blocked), hibernatedFor(wedged)).
		WithStatusSubresource(&cocoonv1.CocoonSet{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{
		block: map[string]chan struct{}{meta.VMNameForPod("ns", "blocked-0"): release},
	}}

	go func() { _, _ = r.Reconcile(context.Background(), reqFor(blocked)) }()

	done := make(chan error, 1)
	go func() { _, err := r.Reconcile(context.Background(), reqFor(wedged)); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unrelated reconcile: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated CocoonSet was blocked by another key's registry probe")
	}
}

func withMainRestore(cs *cocoonv1.CocoonSet) { cs.Finalizers = []string{finalizerName} }

func hibernatedFor(cs *cocoonv1.CocoonSet) *cocoonv1.CocoonHibernation {
	return &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "h-" + cs.Name, Namespace: cs.Namespace},
		Spec: cocoonv1.CocoonHibernationSpec{
			PodRef: cocoonv1.HibernationPodRef{Name: cs.Name + "-0"},
			Desire: cocoonv1.HibernationDesireHibernate,
		},
		Status: cocoonv1.CocoonHibernationStatus{Phase: cocoonv1.CocoonHibernationPhaseHibernated},
	}
}

func countHibernationLists(counter *atomic.Int32) interceptor.Funcs {
	return interceptor.Funcs{
		List: func(ctx context.Context, cli client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*cocoonv1.CocoonHibernationList); ok {
				counter.Add(1)
			}
			return cli.List(ctx, list, opts...)
		},
	}
}

func reqFor(cs *cocoonv1.CocoonSet) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cs.Namespace, Name: cs.Name}}
}
