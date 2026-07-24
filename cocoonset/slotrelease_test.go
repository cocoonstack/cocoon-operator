package cocoonset

import (
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

var (
	relVMName          = meta.VMNameForDeployment("ns", "demo", 0)
	relHibernateTagKey = relVMName + ":" + meta.HibernateSnapshotTag
)

func TestSuspendReleaseRequiresRegistry(t *testing.T) {
	cs := relCocoonSet()
	r := &Reconciler{Scheme: testScheme(t)}
	if _, err := r.reconcileSuspendRelease(t.Context(), cs, classifiedPods{}); err == nil {
		t.Error("release without a registry must fail closed, not release the seat")
	}
}

func TestSuspendReleaseNoPodsSettlesSuspended(t *testing.T) {
	cs := relCocoonSet()
	cli := relClient(t, cs)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: &fakeRegistry{}}

	if _, err := r.reconcileSuspendRelease(t.Context(), cs, classifiedPods{}); err != nil {
		t.Fatalf("reconcileSuspendRelease: %v", err)
	}
	got := mustGetCS(t, cli)
	if got.Status.Phase != cocoonv1.CocoonSetPhaseSuspended {
		t.Errorf("pod-less suspended set must settle Suspended, got %q", got.Status.Phase)
	}
}

func TestSuspendReleaseWaitsForSnapshotVerification(t *testing.T) {
	cs := relCocoonSet()
	pod := relHibernatedPod(t, cs, "node-a")
	cli := relClient(t, cs, pod)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: &fakeRegistry{}} // snapshot absent

	if _, err := r.reconcileSuspendRelease(t.Context(), cs, singlePod(pod)); err != nil {
		t.Fatalf("reconcileSuspendRelease: %v", err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Errorf("pod must survive until the snapshot is registry-verified: %v", err)
	}
}

func TestSuspendReleaseDeletesVerifiedPodAndStashes(t *testing.T) {
	cs := relCocoonSet()
	pod := relHibernatedPod(t, cs, "node-a")
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs, pod)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	if _, err := r.reconcileSuspendRelease(t.Context(), cs, singlePod(pod)); err != nil {
		t.Fatalf("reconcileSuspendRelease: %v", err)
	}
	var gotPod corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &gotPod); !apierrors.IsNotFound(err) {
		t.Errorf("verified hibernated pod must be deleted to release the seat, err=%v", err)
	}
	gotCS := mustGetCS(t, cli)
	if gotCS.Annotations[meta.AnnotationHibernatedOnNode] != "node-a" {
		t.Errorf("must stash the hibernated-on node hint, got %q", gotCS.Annotations[meta.AnnotationHibernatedOnNode])
	}
	if names := gotCS.Annotations[annotationDeleteVMNames]; !strings.Contains(names, relVMName) {
		t.Errorf("must stash vm names for delete-time GC before releasing pods, got %q", names)
	}
	if gotCS.Status.Phase != cocoonv1.CocoonSetPhaseSuspended {
		t.Errorf("Suspended must land with the deletes, got %q", gotCS.Status.Phase)
	}
	if len(reg.deleted) != 0 {
		t.Errorf("suspend must never drop the hibernate snapshot: %v", reg.deleted)
	}
}

func TestSuspendReleaseKeepsTerminalPod(t *testing.T) {
	cs := relCocoonSet()
	pod := mustBuildAgentPod(t, cs, 0, "", "", testScheme(t))
	pod.Spec.NodeName = "node-a"
	meta.LifecycleStatus{State: meta.LifecycleStateFailed, ObservedGeneration: cs.Generation}.Apply(pod)
	cli := relClient(t, cs, pod)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: &fakeRegistry{}}

	if _, err := r.reconcileSuspendRelease(t.Context(), cs, singlePod(pod)); err != nil {
		t.Fatalf("reconcileSuspendRelease: %v", err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Errorf("terminal pod must be kept for triage, not released: %v", err)
	}
	gotCS := mustGetCS(t, cli)
	if gotCS.Status.Phase != cocoonv1.CocoonSetPhaseSuspended {
		t.Errorf("only-terminal set must settle Suspended like retain does, got %q", gotCS.Status.Phase)
	}
}

func TestWakeRecreatesWithRestoreAndPreferredAffinity(t *testing.T) {
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspended
		cs.Annotations = map[string]string{meta.AnnotationHibernatedOnNode: "node-a"}
	})
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileWake(t.Context(), cs, classifiedPods{})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("recreated pod not found: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&got) {
		t.Error("recreated pod must carry restore-from-hibernate")
	}
	if got.Spec.NodeName != "" {
		t.Errorf("wake must not hard-pin a node, got %q", got.Spec.NodeName)
	}
	na := got.Spec.Affinity
	if na == nil || na.NodeAffinity == nil || len(na.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 1 ||
		na.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].Preference.MatchExpressions[0].Values[0] != "node-a" {
		t.Errorf("wake must soft-prefer the hibernated-on node, got %+v", na)
	}
	if na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		t.Error("the node hint must be preferred, never required")
	}
	gotCS := mustGetCS(t, cli)
	if gotCS.Status.Phase != cocoonv1.CocoonSetPhaseWaking {
		t.Errorf("wake must persist the Waking phase before creating the pod, got %q", gotCS.Status.Phase)
	}
	if len(reg.deleted) != 0 {
		t.Errorf("tag must survive until the restored VM is live: %v", reg.deleted)
	}
}

func TestWakeRespectsNodeNamePin(t *testing.T) {
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Spec.NodeName = "node-b"
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspended
		cs.Annotations = map[string]string{meta.AnnotationHibernatedOnNode: "node-a"}
	})
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileWake(t.Context(), cs, classifiedPods{})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("recreated pod not found: %v", err)
	}
	req := got.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if req == nil || req.NodeSelectorTerms[0].MatchExpressions[0].Values[0] != "node-b" {
		t.Errorf("spec.nodeName pin must win over the wake hint, got %+v", got.Spec.Affinity)
	}
}

func TestWakeRestoresOnFastUnsuspendFromSuspending(t *testing.T) {
	// Deletes ran but the phase still reads Suspending: a fast unsuspend here
	// must restore — falling through would fresh-boot over the snapshot.
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspending
		cs.Annotations = map[string]string{meta.AnnotationHibernatedOnNode: "node-a"}
	})
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileWake(t.Context(), cs, classifiedPods{})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("recreated pod not found: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&got) {
		t.Error("fast unsuspend from Suspending must restore, not fresh-boot")
	}
	if mustGetCS(t, cli).Status.Phase != cocoonv1.CocoonSetPhaseWaking {
		t.Error("wake must persist Waking from the Suspending window too")
	}
}

func TestWakeLeavesSuspendingWithoutHintToNormalFlow(t *testing.T) {
	// Suspending without the hint is not a released seat; the normal flow owns it.
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspending
	})
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{probeErr: errors.New("boom")}}
	handled, _, err := r.reconcileWake(t.Context(), cs, classifiedPods{})
	if handled || err != nil {
		t.Errorf("hint-less Suspending must not engage the wake, handled=%v err=%v", handled, err)
	}
}

func TestWakeWaitsOutStalePodCacheThenRestores(t *testing.T) {
	// Fresh CS view (suspend=false, Suspended) with a stale pod cache still
	// showing the deleted hibernated main: wait, then restore once it clears.
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspended
		cs.Annotations = map[string]string{meta.AnnotationHibernatedOnNode: "node-a"}
	})
	stale := relHibernatedPod(t, cs, "node-a")
	meta.HibernateState(true).Apply(stale)
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs) // pod absent: the live read sees the delete
	r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileWake(t.Context(), cs, singlePod(stale))
	if err != nil || !handled {
		t.Fatalf("stale-cache pass: handled=%v err=%v", handled, err)
	}
	if len(reg.deleted) != 0 {
		t.Errorf("waiting must not drop the tag: %v", reg.deleted)
	}

	handled, _, err = r.reconcileWake(t.Context(), cs, classifiedPods{})
	if err != nil || !handled {
		t.Fatalf("post-clear pass: handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("restore pod not found: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&got) {
		t.Error("post-clear pass must restore, not fresh-boot")
	}
}

func TestWakeWaitsOutStaleSuspendingViewThenRestores(t *testing.T) {
	// Delete ran but the Suspended receipt never landed: a stale main view
	// under Suspending must wait, not fresh-boot.
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspending
		cs.Annotations = map[string]string{meta.AnnotationHibernatedOnNode: "node-a"}
	})
	stale := relHibernatedPod(t, cs, "node-a")
	meta.HibernateState(true).Apply(stale)
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs) // pod absent: the live read sees the delete
	r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileWake(t.Context(), cs, singlePod(stale))
	if err != nil || !handled {
		t.Fatalf("stale-view pass: handled=%v err=%v", handled, err)
	}
	handled, _, err = r.reconcileWake(t.Context(), cs, classifiedPods{})
	if err != nil || !handled {
		t.Fatalf("post-clear pass: handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("restore pod not found: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&got) {
		t.Error("receipt-less Suspending must still restore, not fresh-boot")
	}
}

func TestWakeLeavesUndeletedMainToInPlaceWake(t *testing.T) {
	// The delete never finished (pod truly alive): in-place unsuspend owns it.
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspending
		cs.Annotations = map[string]string{meta.AnnotationHibernatedOnNode: "node-a"}
	})
	main := relHibernatedPod(t, cs, "node-a")
	meta.HibernateState(true).Apply(main)
	cli := relClient(t, cs, main)
	r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: &fakeRegistry{}}

	handled, _, err := r.reconcileWake(t.Context(), cs, singlePod(main))
	if handled || err != nil {
		t.Errorf("alive hibernated main must wake in place via the normal flow, handled=%v err=%v", handled, err)
	}
}

func TestWakeFreshBootsWhenNoSnapshot(t *testing.T) {
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspended
	})
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{}}
	handled, _, err := r.reconcileWake(t.Context(), cs, classifiedPods{})
	if err != nil {
		t.Fatalf("reconcileWake: %v", err)
	}
	if handled {
		t.Error("suspended-before-first-boot has nothing to restore; the normal flow owns it")
	}
}

func TestWakeProbeErrorFailsClosed(t *testing.T) {
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspended
	})
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{probeErr: errors.New("boom")}}
	handled, _, err := r.reconcileWake(t.Context(), cs, classifiedPods{})
	if !handled || err == nil {
		t.Errorf("probe failure must own the reconcile (handled=%v err=%v): falling through would fresh-boot over the snapshot", handled, err)
	}
}

func TestWakeWaitsWhileRestoring(t *testing.T) {
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseWaking
	})
	main := migMainPod(t, cs, "", "", false) // unscheduled, no VMID
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs, main)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileWake(t.Context(), cs, singlePod(main))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if len(reg.deleted) != 0 {
		t.Errorf("must not drop the snapshot before the restored VM is live: %v", reg.deleted)
	}
}

func TestWakeDropsTagAndClearsHintWhenLive(t *testing.T) {
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseWaking
		cs.Annotations = map[string]string{meta.AnnotationHibernatedOnNode: "node-a"}
	})
	main := migMainPod(t, cs, "node-c", "vmid-new", true)
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs, main)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileWake(t.Context(), cs, singlePod(main))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if !slices.Contains(reg.deleted, relHibernateTagKey) {
		t.Errorf("live restore must drop the hibernate snapshot, deleted=%v", reg.deleted)
	}
	gotCS := mustGetCS(t, cli)
	if _, ok := gotCS.Annotations[meta.AnnotationHibernatedOnNode]; ok {
		t.Error("node hint must be cleared once the wake completes")
	}
}

func TestWakeIgnoresRunningPhase(t *testing.T) {
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseRunning
	})
	// probeErr proves the registry is never consulted: a stale tag on a
	// running set must not be trusted (it would roll the VM back).
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{probeErr: errors.New("boom")}}
	handled, _, err := r.reconcileWake(t.Context(), cs, classifiedPods{})
	if handled || err != nil {
		t.Errorf("wake must only engage from Suspended/Waking, handled=%v err=%v", handled, err)
	}
}

func TestWakeCompletionSurvivesStalePhase(t *testing.T) {
	// A stale-phase reconcile overwrote Waking mid-wake (e.g. to Pending); the
	// restore-marked live pod + present node hint must keep completion reachable.
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhasePending
		cs.Annotations = map[string]string{meta.AnnotationHibernatedOnNode: "node-a"}
	})
	main := migMainPod(t, cs, "node-a", "vmid-new", true)
	meta.MarkRestoreFromHibernate(main)
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs, main)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileWake(t.Context(), cs, singlePod(main))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if !slices.Contains(reg.deleted, relHibernateTagKey) {
		t.Errorf("completion must survive a stale/overwritten phase, deleted=%v", reg.deleted)
	}
	gotCS := mustGetCS(t, cli)
	if _, ok := gotCS.Annotations[meta.AnnotationHibernatedOnNode]; ok {
		t.Error("hint must be cleared so the fallback retires")
	}
}

func TestWakeWaitsOnStalePhaseWhileRestoring(t *testing.T) {
	// Same staleness race but the VM is not live yet: re-enter the waiting
	// branch (and repaint Waking), do not drop the tag.
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhasePending
		cs.Annotations = map[string]string{meta.AnnotationHibernatedOnNode: "node-a"}
	})
	main := migMainPod(t, cs, "node-a", "", false)
	meta.MarkRestoreFromHibernate(main)
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs, main)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileWake(t.Context(), cs, singlePod(main))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if len(reg.deleted) != 0 {
		t.Errorf("must not drop the snapshot before the restored VM is live: %v", reg.deleted)
	}
}

func TestWakeDisengagesAfterCleanup(t *testing.T) {
	// Steady state: the pod keeps its restore annotation for life but the hint
	// is gone — no re-engage (probeErr proves the registry is never consulted).
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseRunning
	})
	main := migMainPod(t, cs, "node-a", "vmid-new", true)
	meta.MarkRestoreFromHibernate(main)
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{probeErr: errors.New("boom")}}

	handled, _, err := r.reconcileWake(t.Context(), cs, singlePod(main))
	if handled || err != nil {
		t.Errorf("completed wake must hand back to the normal flow, handled=%v err=%v", handled, err)
	}
}

func TestWakeLeavesSuspendedPlaceholderToNormalFlow(t *testing.T) {
	cs := relCocoonSet(func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Suspend = false
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspended
	})
	main := relHibernatedPod(t, cs, "node-a")
	meta.HibernateState(true).Apply(main)
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{}}

	handled, _, err := r.reconcileWake(t.Context(), cs, singlePod(main))
	if handled || err != nil {
		t.Errorf("a suspended set with a placeholder pod wakes in place via applyUnsuspend, handled=%v err=%v", handled, err)
	}
}

func relCocoonSet(mods ...func(*cocoonv1.CocoonSet)) *cocoonv1.CocoonSet {
	return newCocoonSet("demo", append([]func(*cocoonv1.CocoonSet){func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 1
		cs.Spec.Suspend = true
		cs.Spec.HibernatePolicy = cocoonv1.HibernatePolicyRelease
	}}, mods...)...)
}

// relHibernatedPod builds the main pod in the state vk publishes after a
// completed hibernate: lifecycle=hibernated at the CR's generation.
func relHibernatedPod(t *testing.T, cs *cocoonv1.CocoonSet, node string) *corev1.Pod {
	t.Helper()
	pod := mustBuildAgentPod(t, cs, 0, "", "", testScheme(t))
	pod.Spec.NodeName = node
	meta.LifecycleStatus{State: meta.LifecycleStateHibernated, ObservedGeneration: cs.Generation}.Apply(pod)
	return pod
}

func singlePod(pod *corev1.Pod) classifiedPods {
	return classifiedPods{main: pod, allByName: map[string]*corev1.Pod{pod.Name: pod}}
}

func relClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()
	return ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(objs...).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
}

func mustGetCS(t *testing.T, cli client.Client) cocoonv1.CocoonSet {
	t.Helper()
	var cs cocoonv1.CocoonSet
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo"}, &cs); err != nil {
		t.Fatalf("get cocoonset: %v", err)
	}
	return cs
}
