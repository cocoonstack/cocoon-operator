package cocoonset

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// fakeRegistry tracks manifest presence per (name, tag) key so tests can
// simulate snapshots appearing/disappearing.
type fakeRegistry struct {
	present map[string]bool
	probeErr error
}

func (f *fakeRegistry) HasManifest(_ context.Context, name, tag string) (bool, error) {
	if f.probeErr != nil {
		return false, f.probeErr
	}
	return f.present[name+":"+tag], nil
}

func (f *fakeRegistry) DeleteManifest(_ context.Context, _, _ string) error {
	return nil
}

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

func TestEnsureToolboxesCollisionReturnsError(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "0", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest"},
		}
	})

	agentPod := buildAgentPod(cs, 0, "", "", scheme)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agentPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      agentPod,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{agentPod.Name: agentPod},
	}

	_, err := r.ensureToolboxes(t.Context(), cs, classified)
	if err == nil {
		t.Fatal("ensureToolboxes should return error on name collision with agent pod")
	}
}

func TestEnsureToolboxesIdempotentOnExistingToolbox(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest"},
		}
	})

	tbPod := buildToolboxPod(cs, cs.Spec.Toolboxes[0], scheme)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tbPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{},
	}

	changed, err := r.ensureToolboxes(t.Context(), cs, classified)
	if err != nil {
		t.Fatalf("ensureToolboxes: %v", err)
	}
	if changed {
		t.Error("should not report changed for idempotent create")
	}
}

func TestAllOwnedPodsHibernatedWaitsForEachManagedPod(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})
	main := buildAgentPod(cs, 0, "", "", scheme)
	sub := buildAgentPod(cs, 1, "vk-ns-demo-0", "", scheme)
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{1: sub},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main, sub.Name: sub},
	}
	reg := &fakeRegistry{present: map[string]bool{
		"vk-ns-demo-0:" + meta.HibernateSnapshotTag: true,
	}}
	r := &Reconciler{Scheme: scheme, Epoch: reg}

	done, err := r.allOwnedPodsHibernated(t.Context(), classified)
	if err != nil {
		t.Fatalf("allOwnedPodsHibernated: %v", err)
	}
	if done {
		t.Error("must stay pending while sub-agent snapshot is missing")
	}

	reg.present["vk-ns-demo-1:"+meta.HibernateSnapshotTag] = true
	done, err = r.allOwnedPodsHibernated(t.Context(), classified)
	if err != nil {
		t.Fatalf("allOwnedPodsHibernated after sub snapshot: %v", err)
	}
	if !done {
		t.Error("must be done once every managed pod has its snapshot")
	}
}

func TestAllOwnedPodsHibernatedSkipsUnmanagedToolbox(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "tb", Mode: cocoonv1.ToolboxModeStatic, StaticVMID: "qemu-1", StaticIP: "10.0.0.1"},
		}
	})
	main := buildAgentPod(cs, 0, "", "", scheme)
	tb := buildToolboxPod(cs, cs.Spec.Toolboxes[0], scheme)
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{"tb": tb},
		allByName: map[string]*corev1.Pod{main.Name: main, tb.Name: tb},
	}
	// Only main's snapshot is present; static toolbox has no snapshot and must be skipped.
	reg := &fakeRegistry{present: map[string]bool{
		"vk-ns-demo-0:" + meta.HibernateSnapshotTag: true,
	}}
	r := &Reconciler{Scheme: scheme, Epoch: reg}

	done, err := r.allOwnedPodsHibernated(t.Context(), classified)
	if err != nil {
		t.Fatalf("allOwnedPodsHibernated: %v", err)
	}
	if !done {
		t.Error("unmanaged toolbox must not block suspend completion")
	}
}

func TestAllOwnedPodsHibernatedPropagatesProbeError(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	main := buildAgentPod(cs, 0, "", "", scheme)
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main},
	}
	r := &Reconciler{Scheme: scheme, Epoch: &fakeRegistry{probeErr: errors.New("transport boom")}}
	if _, err := r.allOwnedPodsHibernated(t.Context(), classified); err == nil {
		t.Fatal("expected probe error to surface")
	}
}

func TestEnsureSubAgentsReplacesTerminalPod(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})

	subPod := buildAgentPod(cs, 1, "vk-ns-demo-0", "", scheme)
	subPod.Status.Phase = corev1.PodFailed

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(subPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{1: subPod},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{subPod.Name: subPod},
	}

	changed, err := r.ensureSubAgents(t.Context(), cs, classified, "vk-ns-demo-0", "")
	if err != nil {
		t.Fatalf("ensureSubAgents: %v", err)
	}
	if !changed {
		t.Fatal("ensureSubAgents must report changed after deleting a terminal pod")
	}
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: subPod.Namespace, Name: subPod.Name}, &corev1.Pod{}); err == nil {
		t.Error("terminal sub-agent should have been deleted")
	}
}

func TestEnsureToolboxesReplacesTerminalPod(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest"},
		}
	})

	tbPod := buildToolboxPod(cs, cs.Spec.Toolboxes[0], scheme)
	tbPod.Status.Phase = corev1.PodFailed

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tbPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{"tb": tbPod},
		allByName: map[string]*corev1.Pod{tbPod.Name: tbPod},
	}

	changed, err := r.ensureToolboxes(t.Context(), cs, classified)
	if err != nil {
		t.Fatalf("ensureToolboxes: %v", err)
	}
	if !changed {
		t.Fatal("ensureToolboxes must report changed after deleting a terminal pod")
	}
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: tbPod.Namespace, Name: tbPod.Name}, &corev1.Pod{}); err == nil {
		t.Error("terminal toolbox should have been deleted")
	}
}

func TestReconcileDeleteSkipsUnownedPods(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	cs.Finalizers = []string{finalizerName}

	ownedPod := buildAgentPod(cs, 0, "", "", scheme)

	unownedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-stale",
			Namespace: cs.Namespace,
			Labels:    map[string]string{meta.LabelCocoonSet: cs.Name},
		},
	}

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, ownedPod, unownedPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}

	_, err := r.reconcileDelete(t.Context(), cs)
	if err != nil {
		t.Fatalf("reconcileDelete: %v", err)
	}

	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: cs.Namespace, Name: "demo-stale"}, &got); err != nil {
		t.Fatalf("unowned pod should still exist: %v", err)
	}
}

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
