package cocoonset

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

func TestApplyUnsuspendClearsHibernateAnnotation(t *testing.T) {
	scheme := testScheme(t)

	mainPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"},
	}
	meta.HibernateState(true).Apply(mainPod)

	subPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "ns"},
	}
	meta.HibernateState(true).Apply(subPod)

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
	mainPod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	subPod := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0", "", scheme)

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

	agentPod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
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

	_, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
	if err == nil {
		t.Fatal("ensureToolboxes should return error on name collision with agent pod")
	}
}

func TestEnsureToolboxesRejectsDuplicateNames(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest"},
			{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:other"},
		}
	})

	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{},
	}

	_, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
	if err == nil {
		t.Fatal("ensureToolboxes must reject a spec with duplicate toolbox names")
	}
}

func TestEnsureToolboxesIdempotentOnExistingToolbox(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest"},
		}
	})

	tbPod := mustBuildToolboxPod(t, cs, cs.Spec.Toolboxes[0], scheme)
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

	changed, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
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
	main := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	sub := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0", "", scheme)
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{1: sub},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main, sub.Name: sub},
	}
	reg := &fakeRegistry{present: map[string]bool{
		"vk-ns-demo-0:" + meta.HibernateSnapshotTag: true,
	}}
	r := &Reconciler{Scheme: scheme, Registry: reg}

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
	main := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	tb := mustBuildToolboxPod(t, cs, cs.Spec.Toolboxes[0], scheme)
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
	r := &Reconciler{Scheme: scheme, Registry: reg}

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
	main := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main},
	}
	r := &Reconciler{Scheme: scheme, Registry: &fakeRegistry{probeErr: errors.New("transport boom")}}
	if _, err := r.allOwnedPodsHibernated(t.Context(), classified); err == nil {
		t.Fatal("expected probe error to surface")
	}
}

func TestEnsureSubAgentsReplacesTerminalPod(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})

	subPod := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0", "", scheme)
	subPod.Status.Phase = corev1.PodFailed

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, subPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{1: subPod},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{subPod.Name: subPod},
	}

	changed, _, err := r.ensureSubAgents(t.Context(), cs, classified, "vk-ns-demo-0", "", r.newRestoreIntent(t.Context(), cs.Namespace))
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

func TestMainPodFailedReason(t *testing.T) {
	annot := func(state meta.LifecycleState) map[string]string {
		return map[string]string{meta.AnnotationLifecycleState: string(state)}
	}
	cases := []struct {
		name string
		pod  *corev1.Pod
		want string
	}{
		{"healthy", &corev1.Pod{}, ""},
		{"lifecycle=failed annotation", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: annot(meta.LifecycleStateFailed)}}, "PodLifecycleFailed"},
		{"pod phase failed", &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}}, "MainAgentFailed"},
		// lifecycle annotation wins over phase — that's the vk-cocoon-driven path.
		{"both annotation and phase", &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Annotations: annot(meta.LifecycleStateFailed)},
			Status:     corev1.PodStatus{Phase: corev1.PodFailed},
		}, "PodLifecycleFailed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := mainPodFailedReason(c.pod); got != c.want {
				t.Errorf("mainPodFailedReason = %q, want %q", got, c.want)
			}
		})
	}
}

// A main pod carrying lifecycle-state=Failed must flip the CocoonSet to
// Failed even while Pod.Status.Phase is still Running (vk-driven path).
func TestReconcileMainLifecycleFailedTransitionsToFailed(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
	})
	mainPod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	mainPod.Status.Phase = corev1.PodRunning
	if mainPod.Annotations == nil {
		mainPod.Annotations = map[string]string{}
	}
	mainPod.Annotations[meta.AnnotationLifecycleState] = string(meta.LifecycleStateFailed)
	mainPod.Annotations[meta.AnnotationLifecycleStateMessage] = "hibernate push failed"

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, mainPod).
		WithStatusSubresource(&cocoonv1.CocoonSet{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cs.Namespace, Name: cs.Name}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var out cocoonv1.CocoonSet
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: cs.Namespace, Name: cs.Name}, &out); err != nil {
		t.Fatalf("get CocoonSet: %v", err)
	}
	if out.Status.Phase != cocoonv1.CocoonSetPhaseFailed {
		t.Errorf("CocoonSet phase = %q, want Failed", out.Status.Phase)
	}
}

// A sub-agent carrying lifecycle-state=Failed but still PodPhase=Running must
// be rebuilt so the backoff / dead-letter logic runs.
func TestEnsureSubAgentsTreatsLifecycleFailedAsTerminal(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})
	subPod := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0", "", scheme)
	subPod.Status.Phase = corev1.PodRunning
	if subPod.Annotations == nil {
		subPod.Annotations = map[string]string{}
	}
	subPod.Annotations[meta.AnnotationLifecycleState] = string(meta.LifecycleStateFailed)

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, subPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{1: subPod},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{subPod.Name: subPod},
	}

	changed, _, err := r.ensureSubAgents(t.Context(), cs, classified, "vk-ns-demo-0", "", r.newRestoreIntent(t.Context(), cs.Namespace))
	if err != nil {
		t.Fatalf("ensureSubAgents: %v", err)
	}
	if !changed {
		t.Fatal("ensureSubAgents must rebuild a lifecycle-state=Failed sub-agent even when PodPhase is Running")
	}
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: subPod.Namespace, Name: subPod.Name}, &corev1.Pod{}); err == nil {
		t.Error("failed sub-agent should have been deleted")
	}
}

func TestEnsureToolboxesReplacesTerminalPod(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest"},
		}
	})

	tbPod := mustBuildToolboxPod(t, cs, cs.Spec.Toolboxes[0], scheme)
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

	changed, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
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

	ownedPod := mustBuildAgentPod(t, cs, 0, "", "", scheme)

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

// Teardown always GCs :hibernate; :latest only when snapshotPolicy says no
// push happened for that slot (always/main-only-slot-0 keep it for retag).
func TestReconcileDeleteSnapshotPolicyGC(t *testing.T) {
	scheme := testScheme(t)
	cases := []struct {
		name      string
		policy    cocoonv1.SnapshotPolicy
		agents    []cocoonv1.AgentStatus
		toolboxes []cocoonv1.ToolboxStatus
		want      []string
	}{
		{
			name:   "never drops both tags — no push happened",
			policy: cocoonv1.SnapshotPolicyNever,
			agents: []cocoonv1.AgentStatus{
				{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0"},
			},
			want: []string{
				"vk-ns-demo-0:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-0:" + meta.DefaultSnapshotTag,
			},
		},
		{
			name:   "always preserves :latest for downstream retag",
			policy: cocoonv1.SnapshotPolicyAlways,
			agents: []cocoonv1.AgentStatus{
				{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0"},
			},
			want: []string{"vk-ns-demo-0:" + meta.HibernateSnapshotTag},
		},
		{
			name:   "main-only keeps slot 0, drops other slots and toolboxes",
			policy: cocoonv1.SnapshotPolicyMainOnly,
			agents: []cocoonv1.AgentStatus{
				{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0"},
				{Slot: 1, Role: "sub", PodName: "demo-1", VMName: "vk-ns-demo-1"},
			},
			toolboxes: []cocoonv1.ToolboxStatus{
				{Name: "tb", PodName: "demo-tb", VMName: "vk-ns-demo-tb"},
			},
			want: []string{
				"vk-ns-demo-0:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-1:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-1:" + meta.DefaultSnapshotTag,
				"vk-ns-demo-tb:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-tb:" + meta.DefaultSnapshotTag,
			},
		},
		{
			name:   "main-only reclaims :latest for toolbox with numeric-suffix name",
			policy: cocoonv1.SnapshotPolicyMainOnly,
			agents: []cocoonv1.AgentStatus{
				{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0"},
			},
			toolboxes: []cocoonv1.ToolboxStatus{
				{Name: "db-0", PodName: "demo-db-0", VMName: "vk-ns-demo-db-0"},
			},
			want: []string{
				"vk-ns-demo-0:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-db-0:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-db-0:" + meta.DefaultSnapshotTag,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cs := newCocoonSet("demo")
			cs.Finalizers = []string{finalizerName}
			cs.Spec.SnapshotPolicy = c.policy
			cs.Status.Agents = c.agents
			cs.Status.Toolboxes = c.toolboxes

			cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs).Build()
			reg := &fakeRegistry{}
			r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}

			if _, err := r.reconcileDelete(t.Context(), cs); err != nil {
				t.Fatalf("reconcileDelete: %v", err)
			}
			if !slices.Equal(reg.deleted, c.want) {
				t.Errorf("DeleteManifest calls = %v, want %v", reg.deleted, c.want)
			}
		})
	}
}

// Race window: pod exists but Status.Agents lacks VMName yet — pass 1 stashes
// the pod's VMName onto the annotation so pass 2 still GCs :hibernate.
func TestReconcileDeleteStashesPodVMNamesEvenWhenStatusIsEmpty(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	cs.Finalizers = []string{finalizerName}

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, mustBuildAgentPod(t, cs, 0, "", "", scheme)).
		Build()
	reg := &fakeRegistry{}
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}

	if _, err := r.reconcileDelete(t.Context(), cs); err != nil {
		t.Fatalf("reconcileDelete: %v", err)
	}

	want := []string{"vk-ns-demo-0:" + meta.HibernateSnapshotTag}
	if !slices.Equal(reg.deleted, want) {
		t.Errorf("DeleteManifest calls = %v, want %v", reg.deleted, want)
	}
}

// Real clusters terminate pods asynchronously: by the time GC runs, the pod
// list is already empty. VM names must come from Status, not from a re-list.
func TestReconcileDeleteCleansTagsAfterPodsGone(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	cs.Finalizers = []string{finalizerName}
	cs.Status.Agents = []cocoonv1.AgentStatus{
		{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0"},
		{Slot: 1, Role: "sub", PodName: "demo-1", VMName: "vk-ns-demo-1"},
	}
	cs.Status.Toolboxes = []cocoonv1.ToolboxStatus{
		{Name: "tb", PodName: "demo-tb", VMName: "vk-ns-demo-tb"},
	}

	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs).Build()
	reg := &fakeRegistry{}
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}

	if _, err := r.reconcileDelete(t.Context(), cs); err != nil {
		t.Fatalf("reconcileDelete: %v", err)
	}

	// Default policy is always, so :latest is preserved for every VM.
	want := []string{
		"vk-ns-demo-0:" + meta.HibernateSnapshotTag,
		"vk-ns-demo-1:" + meta.HibernateSnapshotTag,
		"vk-ns-demo-tb:" + meta.HibernateSnapshotTag,
	}
	if !slices.Equal(reg.deleted, want) {
		t.Errorf("DeleteManifest calls = %v, want %v", reg.deleted, want)
	}
}

func TestApplyUnsuspendSkipsPodHibernatedByCR(t *testing.T) {
	scheme := testScheme(t)

	hibernated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"},
	}
	meta.HibernateState(true).Apply(hibernated)

	// Also hibernated but not named in any CR -- proves skip is selective.
	leftover := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "ns"},
	}
	meta.HibernateState(true).Apply(leftover)

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

// A nil manager suffices: the guard rejects before mgr is touched.
func TestSetupWithManagerRejectsInvalidConcurrency(t *testing.T) {
	for _, n := range []int{0, -1} {
		if err := (&Reconciler{Concurrency: n}).SetupWithManager(t.Context(), nil); err == nil {
			t.Errorf("concurrency %d must be rejected", n)
		}
	}
}

type fakeRegistry struct {
	present  map[string]bool
	probeErr error
	// delay models a remote round trip; block wedges the named VM's probe.
	delay     time.Duration
	block     map[string]chan struct{}
	deletedMu sync.Mutex
	deleted   []string
}

func (f *fakeRegistry) HasManifest(_ context.Context, name, tag string) (bool, error) {
	if f.probeErr != nil {
		return false, f.probeErr
	}
	if ch, ok := f.block[name]; ok {
		<-ch
	}
	time.Sleep(f.delay)
	return f.present[name+":"+tag], nil
}

func (f *fakeRegistry) DeleteManifest(_ context.Context, name, tag string) error {
	f.deletedMu.Lock()
	defer f.deletedMu.Unlock()
	f.deleted = append(f.deleted, name+":"+tag)
	return nil
}
