package cocoonset

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/manifest"
	"github.com/cocoonstack/cocoon-common/meta"
	commonsnapshot "github.com/cocoonstack/cocoon-common/snapshot"
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
	cs := newCocoonSet("demo")

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, mainPod, subPod, tbPod).
		Build()

	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      mainPod,
		sub:       map[int32]*corev1.Pod{1: subPod},
		toolbox:   map[string]*corev1.Pod{"tb": tbPod},
		allByName: map[string]*corev1.Pod{"demo-0": mainPod, "demo-1": subPod, "demo-tb": tbPod},
	}

	if err := r.applyUnsuspend(t.Context(), cs, classified); err != nil {
		t.Fatalf("applyUnsuspend: %v", err)
	}

	for _, name := range []string{"demo-0", "demo-1", "demo-tb"} {
		t.Run(name, func(t *testing.T) {
			var got corev1.Pod
			if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: name}, &got); err != nil {
				t.Fatalf("get %s: %v", name, err)
			}
			if meta.ReadHibernateState(&got) {
				t.Errorf("%s: hibernate annotation must be clear", name)
			}
		})
	}
}

func TestApplyUnsuspendNoopOnCleanSet(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	mainPod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	subPod := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0-505043", "", scheme)

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, mainPod, subPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      mainPod,
		sub:       map[int32]*corev1.Pod{1: subPod},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{mainPod.Name: mainPod, subPod.Name: subPod},
	}

	if err := r.applyUnsuspend(t.Context(), cs, classified); err != nil {
		t.Errorf("applyUnsuspend on clean set: %v", err)
	}
	if _, owed := mustGetCS(t, cli).Annotations[annotationHibernateReclaim]; owed {
		t.Errorf("a set with nothing to wake owes no reclaim")
	}
}

func TestApplyUnsuspendRecordsTheVMsItWakes(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 3
		cs.Annotations = map[string]string{annotationHibernateReclaim: `{"generation":1,"vms":["vk-ns-old"]}`}
	})
	pods := []corev1.Pod{*rehibernated(readyAt(t, 0, 3)), *rehibernated(readyAt(t, 1, 3))}
	var setPatches atomic.Int32
	cli := relInterceptedClient(t, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, isSet := obj.(*cocoonv1.CocoonSet); isSet {
				setPatches.Add(1)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}, cs, &pods[0], &pods[1])
	r := &Reconciler{Client: cli, Scheme: testScheme(t)}

	for range 2 {
		stale := []corev1.Pod{*pods[0].DeepCopy(), *pods[1].DeepCopy()}
		if err := r.applyUnsuspend(t.Context(), cs, classifyPods(stale)); err != nil {
			t.Fatalf("applyUnsuspend: %v", err)
		}
	}
	got := readHibernateReclaim(new(mustGetCS(t, cli)))
	if want := append([]string{"vk-ns-old"}, slotNames([]int32{0, 1}, "")...); got.Generation != 3 || !slices.Equal(got.VMs, want) {
		t.Errorf("owed reclaim = %+v, want generation 3 and %v", got, want)
	}
	if n := setPatches.Load(); n != 1 {
		t.Errorf("a pass over a stale pod cache must not rewrite an unchanged record, set patched %d times", n)
	}
}

func TestApplyUnsuspendRecordsTheReclaimBeforeItClears(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) { cs.Generation = 2 })
	pod := rehibernated(readyAt(t, 0, 2))
	cli := relInterceptedClient(t, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return errors.New("pod patch refused")
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}, cs, pod)
	r := &Reconciler{Client: cli, Scheme: testScheme(t)}

	if err := r.applyUnsuspend(t.Context(), cs, singlePod(pod)); err == nil {
		t.Fatalf("applyUnsuspend must surface the refused pod patch")
	}
	if got := readHibernateReclaim(new(mustGetCS(t, cli))); !slices.Contains(got.VMs, relVMName) {
		t.Errorf("the reclaim must be recorded before any hibernate annotation clears, got %+v", got)
	}
}

func TestApplySuspendRecordsEveryVMBeforeItHibernates(t *testing.T) {
	tb := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:1", Mode: cocoonv1.ToolboxModeRun}
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 2
		cs.Spec.Suspend = true
		cs.Spec.Agent.Replicas = 1
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{tb}
		cs.Annotations = map[string]string{annotationHibernateReclaim: `{"generation":1,"vms":["vk-ns-old"]}`}
	})
	pods := []corev1.Pod{
		*mustBuildAgentPod(t, cs, 0, "", "", testScheme(t)),
		*mustBuildAgentPod(t, cs, 1, relVMName, "", testScheme(t)),
		*mustBuildToolboxPod(t, cs, tb, testScheme(t)),
	}
	cli := relInterceptedClient(t, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return errors.New("pod patch refused")
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}, cs, &pods[0], &pods[1], &pods[2])
	r := &Reconciler{Client: cli, Scheme: testScheme(t)}

	if err := r.applySuspend(t.Context(), cs, classifyPods(pods)); err == nil {
		t.Fatal("applySuspend must surface the refused pod patch")
	}
	got := readHibernateReclaim(new(mustGetCS(t, cli)))
	want := slices.Concat([]string{"vk-ns-old"}, slotNames([]int32{0, 1}, ""), []string{meta.VMNameForPod("ns", meta.ToolboxPodName("demo", tb.Name))})
	if !got.Suspended || !slices.Equal(got.VMs, want) {
		t.Errorf("record %+v, want the suspend marker and %v written before any hibernate annotation", got, want)
	}
}

func TestReclaimWokenSnapshotsReclaimsEachVMOnceItWakes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		owed        []int32
		generation  int64
		probeErr    error
		pods        []*corev1.Pod
		wantDeleted []int32
		wantOwed    []int32
		wantErr     bool
	}{
		{name: "woken at the unsuspend generation", pods: []*corev1.Pod{wokenPod(t, 0, 2)}, wantDeleted: []int32{0}},
		{name: "still hibernated", pods: []*corev1.Pod{rehibernated(wokenPod(t, 0, 2))}, wantOwed: []int32{0}},
		{name: "ready before the unsuspend", pods: []*corev1.Pod{wokenPod(t, 0, 1)}, wantOwed: []int32{0}},
		{name: "ready without a live VM", pods: []*corev1.Pod{readyAt(t, 0, 2)}, wantOwed: []int32{0}},
		{name: "hibernating again in vk", pods: []*corev1.Pod{inState(wokenPod(t, 0, 2), meta.LifecycleStateHibernating)}, wantOwed: []int32{0}},
		{name: "rebuilt mid-wake", wantOwed: []int32{0}},
		{name: "a spec edit after the wake", generation: 3, pods: []*corev1.Pod{wokenPod(t, 0, 2)}, wantDeleted: []int32{0}},
		{name: "one woken, one not", owed: []int32{0, 1}, pods: []*corev1.Pod{rehibernated(wokenPod(t, 0, 2)), wokenPod(t, 1, 2)}, wantDeleted: []int32{1}, wantOwed: []int32{0}},
		{name: "registry refuses", probeErr: errors.New("registry down"), pods: []*corev1.Pod{wokenPod(t, 0, 2)}, wantOwed: []int32{0}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owed := tc.owed
			if owed == nil {
				owed = []int32{0}
			}
			cs := reclaimOwedSet(t, owed...)
			cs.Generation = cmp.Or(tc.generation, cs.Generation)
			reg := &fakeRegistry{probeErr: tc.probeErr, present: map[string]bool{}}
			for _, tag := range slotNames([]int32{0, 1}, ":"+meta.HibernateSnapshotTag) {
				reg.present[tag] = true
			}
			objs := []client.Object{cs}
			classified := classifiedPods{allByName: map[string]*corev1.Pod{}}
			for _, p := range tc.pods {
				objs = append(objs, p)
				classified.allByName[p.Name] = p
			}
			r := &Reconciler{Client: relClient(t, objs...), Scheme: testScheme(t), Registry: reg}

			if err := r.reclaimWokenSnapshots(t.Context(), cs, classified); (err != nil) != tc.wantErr {
				t.Fatalf("reclaimWokenSnapshots error %v, want error %v", err, tc.wantErr)
			}
			if want := slotNames(tc.wantDeleted, ":"+meta.HibernateSnapshotTag); !slices.Equal(reg.deleted, want) {
				t.Errorf("deleted %v, want %v", reg.deleted, want)
			}
			if got, want := readHibernateReclaim(new(mustGetCS(t, r.Client))).VMs, slotNames(tc.wantOwed, ""); !slices.Equal(got, want) {
				t.Errorf("still owed %v, want %v", got, want)
			}
		})
	}
}

func TestApplyUnsuspendRecordsARestoreForASubAgentMissingAtUnsuspend(t *testing.T) {
	vm1 := slotNames([]int32{1}, "")[0]
	for _, tc := range []struct {
		name        string
		subPresent  bool
		recorded    []int32
		wantRestore bool
	}{
		{name: "missing slot the suspend recorded", recorded: []int32{0, 1}, wantRestore: true},
		{name: "missing slot the suspend never saw", recorded: []int32{0}},
		{name: "every slot present", subPresent: true, recorded: []int32{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
				cs.Generation = 2
				cs.Spec.Agent.Replicas = 1
				cs.Annotations = map[string]string{annotationHibernateReclaim: encodeReclaim(t, hibernateReclaim{VMs: slotNames(tc.recorded, ""), Suspended: true})}
			})
			pods := []corev1.Pod{*rehibernated(mustBuildAgentPod(t, cs, 0, "", "", testScheme(t)))}
			if tc.subPresent {
				pods = append(pods, *rehibernated(mustBuildAgentPod(t, cs, 1, relVMName, "", testScheme(t))))
			}
			objs := []client.Object{cs}
			for i := range pods {
				objs = append(objs, &pods[i])
			}
			reg := &fakeRegistry{present: map[string]bool{vm1 + ":" + meta.HibernateSnapshotTag: true}}
			cli := relClient(t, objs...)
			r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

			if err := r.applyUnsuspend(t.Context(), cs, classifyPods(pods)); err != nil {
				t.Fatalf("applyUnsuspend: %v", err)
			}
			owed := readHibernateReclaim(new(mustGetCS(t, cli)))
			if restores := slices.Contains(owed.Restore, vm1) && slices.Contains(owed.VMs, vm1); restores != tc.wantRestore {
				t.Errorf("record %+v owes a restore of %s = %v, want %v", owed, vm1, restores, tc.wantRestore)
			}
			if owed.Suspended || owed.Generation != 2 {
				t.Errorf("record %+v, want generation 2 and the suspend marker cleared", owed)
			}
			if len(reg.probed) != 0 {
				t.Errorf("the unsuspend must not probe the registry, probed %v", reg.probed)
			}
		})
	}
}

func TestUnsuspendReclaimsTheSnapshotOfAToolboxDeletedWhileSuspended(t *testing.T) {
	tb := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:1", Mode: cocoonv1.ToolboxModeRun}
	tbVM := meta.VMNameForPod("ns", meta.ToolboxPodName("demo", tb.Name))
	tbTag := tbVM + ":" + meta.HibernateSnapshotTag
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 2
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{tb}
		cs.Annotations = map[string]string{annotationHibernateReclaim: encodeReclaim(t, hibernateReclaim{VMs: []string{relVMName, tbVM}, Suspended: true})}
	})
	main := rehibernated(mustBuildAgentPod(t, cs, 0, "", "", testScheme(t)))
	reg := &fakeRegistry{present: map[string]bool{tbTag: true}, images: map[string]string{tbTag: tb.Image}}
	cli := relClient(t, cs, main)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	if err := r.applyUnsuspend(t.Context(), cs, singlePod(main)); err != nil {
		t.Fatalf("applyUnsuspend: %v", err)
	}
	if owed := readHibernateReclaim(new(mustGetCS(t, cli))); !slices.Contains(owed.VMs, tbVM) || !slices.Contains(owed.Restore, tbVM) {
		t.Fatalf("record %+v, want %s owed a restore and a reclaim", owed, tbVM)
	}

	if _, _, err := r.ensureToolboxes(t.Context(), cs, singlePod(main), r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
		t.Fatalf("ensureToolboxes: %v", err)
	}
	var woken corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: meta.ToolboxPodName("demo", tb.Name)}, &woken); err != nil {
		t.Fatalf("the toolbox must be recreated: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&woken) || slices.Contains(reg.deleted, tbTag) {
		t.Fatalf("the toolbox must be created restore-marked over its own snapshot, deleted %v", reg.deleted)
	}
	meta.LifecycleStatus{State: meta.LifecycleStateReady, ObservedGeneration: 2}.Apply(&woken)
	meta.VMRuntime{VMID: "vm-woken"}.Apply(&woken)
	woken.Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := r.reclaimWokenSnapshots(t.Context(), cs, classifyPods([]corev1.Pod{woken})); err != nil {
		t.Fatalf("reclaimWokenSnapshots: %v", err)
	}
	if !slices.Contains(reg.deleted, tbTag) {
		t.Errorf("the woken toolbox's %s must be reclaimed, deleted %v", tbTag, reg.deleted)
	}
}

func TestReclaimWokenSnapshotsDropsAWokenRestoreFromTheRecord(t *testing.T) {
	cs := reclaimOwedSet(t, 0, 1)
	cs.Annotations[annotationHibernateReclaim] = encodeReclaim(t, hibernateReclaim{Generation: 2, VMs: slotNames([]int32{0, 1}, ""), Restore: slotNames([]int32{1}, "")})
	woken := wokenPod(t, 1, 2)
	r := &Reconciler{Client: relClient(t, cs, woken), Scheme: testScheme(t), Registry: &fakeRegistry{}}

	if err := r.reclaimWokenSnapshots(t.Context(), cs, classifyPods([]corev1.Pod{*woken})); err != nil {
		t.Fatalf("reclaimWokenSnapshots: %v", err)
	}
	if got := readHibernateReclaim(new(mustGetCS(t, r.Client))); !slices.Equal(got.VMs, slotNames([]int32{0}, "")) || len(got.Restore) != 0 {
		t.Errorf("record %+v, want only slot 0 owed and no restore left", got)
	}
}

func TestReconcileReclaimsTheTagAPlainUnsuspendWoke(t *testing.T) {
	cs := reclaimOwedSet(t, 0)
	cs.Finalizers = []string{finalizerName}
	reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
	cli := relClient(t, cs, wokenPod(t, 0, 2))
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !slices.Contains(reg.deleted, relHibernateTagKey) {
		t.Fatalf("the woken VM's %s must be reclaimed, deleted %v", relHibernateTagKey, reg.deleted)
	}
	if _, owed := mustGetCS(t, cli).Annotations[annotationHibernateReclaim]; owed {
		t.Errorf("the reclaim must settle once every owed VM is reclaimed")
	}

	reg.probed = nil
	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if len(reg.probed) != 0 {
		t.Errorf("a settled set must not probe the registry, probed %v", reg.probed)
	}
}

func TestReconcileUnsuspendWithEveryPodGoneRestoresASubAgentFromItsSnapshot(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
		cs.Generation = 3
		cs.Spec.Suspend = true
		cs.Spec.Agent.Replicas = 1
	})
	main, sub := agentPair(t, cs)
	tags := slotNames([]int32{0, 1}, ":"+meta.HibernateSnapshotTag)
	reg := &fakeRegistry{present: map[string]bool{tags[0]: true, tags[1]: true}}
	cli := relClient(t, cs, lifecycleHibernated(main), lifecycleHibernated(sub))
	r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: reg}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("suspend pass: %v", err)
	}
	if phase := mustGetCS(t, cli).Status.Phase; phase != cocoonv1.CocoonSetPhaseSuspended {
		t.Fatalf("phase = %q, want Suspended", phase)
	}
	for _, pod := range []*corev1.Pod{main, sub} {
		if err := cli.Delete(t.Context(), pod); err != nil {
			t.Fatalf("delete %s: %v", pod.Name, err)
		}
	}
	unsuspend(t, cli, nil)
	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("wake pass: %v", err)
	}
	markWoken(t, cli, "demo-0", "node-a", 4)
	for i := range 3 {
		if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
			t.Fatalf("pass %d: %v", i+2, err)
		}
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-1"}, &got); err != nil {
		t.Fatalf("the sub-agent must be recreated: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&got) {
		t.Error("the sub-agent must restore its snapshot, since vk-cocoon refuses a fork-from pod while the tag exists")
	}
	if !reg.present[tags[1]] {
		t.Errorf("the snapshot the sub-agent restores from must be kept, deleted %v", reg.deleted)
	}
}

func TestReconcileUnsuspendRestoresAMainDeletedBeforeTheSuspendSettled(t *testing.T) {
	for _, tc := range []struct {
		name      string
		timedOut  bool
		wantPhase cocoonv1.CocoonSetPhase
	}{
		{name: "suspending", wantPhase: cocoonv1.CocoonSetPhaseSuspending},
		{name: "suspend timed out", timedOut: true, wantPhase: cocoonv1.CocoonSetPhaseFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
				cs.Finalizers = []string{finalizerName}
				cs.Generation = 3
				cs.Spec.Suspend = true
				cs.Spec.Agent.Replicas = 1
				if tc.timedOut {
					cs.Annotations = map[string]string{annotationSuspendingSince: time.Now().Add(-suspendTimeout - time.Minute).UTC().Format(time.RFC3339)}
					cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspending
				}
			})
			main, sub := agentPair(t, cs)
			tags := slotNames([]int32{0, 1}, ":"+meta.HibernateSnapshotTag)
			reg := &fakeRegistry{present: map[string]bool{tags[0]: true, tags[1]: true}}
			cli := relClient(t, cs, main, sub)
			r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: reg}

			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
				t.Fatalf("suspend pass: %v", err)
			}
			if phase := mustGetCS(t, cli).Status.Phase; phase != tc.wantPhase {
				t.Fatalf("phase = %q, want %q", phase, tc.wantPhase)
			}
			if err := cli.Delete(t.Context(), main); err != nil {
				t.Fatalf("delete main: %v", err)
			}
			unsuspend(t, cli, nil)
			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
				t.Fatalf("unsuspend pass: %v", err)
			}
			var got corev1.Pod
			if err := cli.Get(t.Context(), client.ObjectKeyFromObject(main), &got); err != nil {
				t.Fatalf("the main must be recreated: %v", err)
			}
			if !meta.ReadRestoreFromHibernate(&got) {
				t.Error("the recreated main must restore its suspend snapshot")
			}
			if owed := readHibernateReclaim(new(mustGetCS(t, cli))); !slices.Contains(owed.VMs, relVMName) {
				t.Fatalf("record %+v, want the main VM owed a reclaim", owed)
			}

			markWoken(t, cli, "demo-0", "node-a", 4)
			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
				t.Fatalf("woken pass: %v", err)
			}
			if !slices.Contains(reg.deleted, tags[0]) {
				t.Errorf("the woken main's %s must be reclaimed so a later recreate boots fresh, deleted %v", tags[0], reg.deleted)
			}
		})
	}
}

func TestReconcileRebuildOfAnOwedMainDropsTheSnapshotOnlyAcrossAnImageChange(t *testing.T) {
	for _, tc := range []struct {
		name        string
		edit        func(*cocoonv1.CocoonSet)
		wantDropped bool
	}{
		{name: "image edit", edit: func(cs *cocoonv1.CocoonSet) { cs.Spec.Agent.Image = "ghcr.io/cocoonstack/cocoon/ubuntu:26.04" }, wantDropped: true},
		{name: "service account edit", edit: func(cs *cocoonv1.CocoonSet) { cs.Spec.Agent.ServiceAccountName = "agent" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := newCocoonSet("demo", tc.edit, func(cs *cocoonv1.CocoonSet) {
				cs.Finalizers = []string{finalizerName}
				cs.Generation = 2
				cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspended
			})
			main := rehibernated(lifecycleHibernated(mustBuildAgentPod(t, newCocoonSet("demo"), 0, "", "", testScheme(t))))
			reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}, images: map[string]string{relHibernateTagKey: meta.ParseVMSpec(main).Image}}
			cli := relClient(t, cs, main)
			r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: reg}

			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
				t.Fatalf("unsuspend pass: %v", err)
			}
			if err := cli.Get(t.Context(), client.ObjectKeyFromObject(main), &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Fatalf("the drifted main must be deleted for rebuild, got err=%v", err)
			}

			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
				t.Fatalf("recreate pass: %v", err)
			}
			var fresh corev1.Pod
			if err := cli.Get(t.Context(), client.ObjectKeyFromObject(main), &fresh); err != nil {
				t.Fatalf("the main must be recreated: %v", err)
			}
			if restores := meta.ReadRestoreFromHibernate(&fresh); restores == tc.wantDropped {
				t.Errorf("recreated main restores from :hibernate = %v, want %v", restores, !tc.wantDropped)
			}
			if dropped := slices.Contains(reg.deleted, relHibernateTagKey); dropped != tc.wantDropped {
				t.Errorf("%s dropped = %v, want %v", relHibernateTagKey, dropped, tc.wantDropped)
			}
			if _, owed := mustGetCS(t, cli).Annotations[annotationHibernateReclaim]; owed == tc.wantDropped {
				t.Errorf("reclaim record kept = %v, want %v", owed, !tc.wantDropped)
			}
		})
	}
}

func TestReconcileRecreatesAnImageDriftedMainOnlyOnceItsSnapshotIsDropped(t *testing.T) {
	for _, tc := range []struct {
		name      string
		probeErr  error
		deleteErr error
	}{
		{name: "registry down", probeErr: errors.New("registry down")},
		{name: "delete refused", deleteErr: errors.New("delete refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := reclaimOwedSet(t, 0)
			cs.Finalizers = []string{finalizerName}
			cs.Spec.Agent.Image = "ghcr.io/cocoonstack/cocoon/ubuntu:26.04"
			reg := &fakeRegistry{
				present:   map[string]bool{relHibernateTagKey: true},
				images:    map[string]string{relHibernateTagKey: "ghcr.io/cocoonstack/cocoon/ubuntu:24.04"},
				probeErr:  tc.probeErr,
				deleteErr: tc.deleteErr,
			}
			cli := relClient(t, cs)
			r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: reg}

			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err == nil {
				t.Fatal("a refused snapshot drop must fail the pass so it retries with backoff")
			}
			if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Errorf("no main may be created over a snapshot it cannot restore, got err=%v", err)
			}
			if got := readHibernateReclaim(new(mustGetCS(t, cli))).VMs; !slices.Equal(got, slotNames([]int32{0}, "")) {
				t.Errorf("still owed %v, want the main kept recorded", got)
			}

			reg.probeErr, reg.deleteErr = nil, nil
			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
				t.Fatalf("retry: %v", err)
			}
			var fresh corev1.Pod
			if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &fresh); err != nil || meta.ReadRestoreFromHibernate(&fresh) {
				t.Errorf("the retry must boot the main fresh, err=%v restore=%v", err, meta.ReadRestoreFromHibernate(&fresh))
			}
			if _, owed := mustGetCS(t, cli).Annotations[annotationHibernateReclaim]; owed {
				t.Error("the dropped VM must leave the reclaim record")
			}
		})
	}
}

func TestReconcileSuspendRecreatesAMissingMainFreshOverASnapshotFromAnotherImage(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
		cs.Generation = 2
		cs.Spec.Suspend = true
		cs.Spec.Agent.Image = "ghcr.io/cocoonstack/cocoon/ubuntu:26.04"
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspended
	})
	reg := &fakeRegistry{
		present: map[string]bool{relHibernateTagKey: true},
		images:  map[string]string{relHibernateTagKey: "ghcr.io/cocoonstack/cocoon/ubuntu:24.04"},
	}
	cli := relClient(t, cs)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("the missing main must be recreated: %v", err)
	}
	if meta.ReadRestoreFromHibernate(&got) {
		t.Error("a main whose snapshot came from another image must boot fresh, since vk-cocoon refuses the restore")
	}
	if !slices.Contains(reg.deleted, relHibernateTagKey) {
		t.Errorf("the snapshot from another image must be dropped, deleted %v", reg.deleted)
	}
}

func TestEnsureToolboxesCreatesAToolboxFreshOverASnapshotFromAnotherImage(t *testing.T) {
	pushed := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:1", Mode: cocoonv1.ToolboxModeRun}
	vm := meta.VMNameForPod("ns", meta.ToolboxPodName("demo", pushed.Name))
	tagKey := vm + ":" + meta.HibernateSnapshotTag
	for _, tc := range []struct {
		name        string
		image       string
		rebuilt     bool
		owed        bool
		wantDropped bool
	}{
		{name: "rebuilt for an image edit while owed a reclaim", image: "ghcr.io/cocoonstack/cocoon/toolbox:2", rebuilt: true, owed: true, wantDropped: true},
		{name: "missing after an image edit", image: "ghcr.io/cocoonstack/cocoon/toolbox:2", wantDropped: true},
		{name: "missing with a snapshot of its own image", image: pushed.Image},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tb := pushed
			tb.Image = tc.image
			cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
				cs.Generation = 2
				cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{tb}
				if tc.owed {
					cs.Annotations = map[string]string{annotationHibernateReclaim: encodeReclaim(t, hibernateReclaim{Generation: 2, VMs: []string{vm}})}
				}
			})
			var pods []corev1.Pod
			objs := []client.Object{cs}
			if tc.rebuilt {
				pods = append(pods, *mustBuildToolboxPod(t, newCocoonSet("demo"), pushed, testScheme(t)))
				objs = append(objs, &pods[0])
			}
			reg := &fakeRegistry{present: map[string]bool{tagKey: true}, images: map[string]string{tagKey: pushed.Image}}
			cli := relClient(t, objs...)
			r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

			if _, _, err := r.ensureToolboxes(t.Context(), cs, classifyPods(pods), r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
				t.Fatalf("ensureToolboxes: %v", err)
			}
			if _, _, err := r.ensureToolboxes(t.Context(), cs, classifyPods(nil), r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
				t.Fatalf("ensureToolboxes recreate: %v", err)
			}
			var got corev1.Pod
			if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: meta.ToolboxPodName("demo", tb.Name)}, &got); err != nil {
				t.Fatalf("the toolbox must be created: %v", err)
			}
			if dropped := slices.Contains(reg.deleted, tagKey); dropped != tc.wantDropped {
				t.Errorf("%s dropped = %v, want %v", tagKey, dropped, tc.wantDropped)
			}
			if _, owed := mustGetCS(t, cli).Annotations[annotationHibernateReclaim]; tc.owed && owed == tc.wantDropped {
				t.Errorf("reclaim record kept = %v, want %v", owed, !tc.wantDropped)
			}
		})
	}
}

func TestEnsureToolboxesRejectsAnInvalidSpec(t *testing.T) {
	for _, tc := range []struct {
		name      string
		toolboxes []cocoonv1.ToolboxSpec
		squatter  bool
		want      string
	}{
		{name: "name taken by a pod that is not the toolbox", toolboxes: []cocoonv1.ToolboxSpec{{Name: "tb"}}, squatter: true, want: "name collision"},
		{name: "integer name", toolboxes: []cocoonv1.ToolboxSpec{{Name: "1"}}, want: "must not be an integer"},
		{name: "duplicate names", toolboxes: []cocoonv1.ToolboxSpec{{Name: "tb"}, {Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:other"}}, want: "duplicate toolbox name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := testScheme(t)
			cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) { cs.Spec.Toolboxes = tc.toolboxes })
			classified := classifiedPods{sub: map[int32]*corev1.Pod{}, toolbox: map[string]*corev1.Pod{}, allByName: map[string]*corev1.Pod{}}
			builder := ctrlfake.NewClientBuilder().WithScheme(scheme)
			if tc.squatter {
				pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
				pod.Name = meta.ToolboxPodName(cs.Name, "tb")
				classified.allByName[pod.Name] = pod
				builder = builder.WithObjects(pod)
			}
			r := &Reconciler{Client: builder.Build(), Scheme: scheme}

			_, _, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ensureToolboxes err = %v, want one containing %q", err, tc.want)
			}
		})
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
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{},
	}

	changed, _, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
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
	main := lifecycleHibernated(mustBuildAgentPod(t, cs, 0, "", "", scheme))
	sub := lifecycleHibernated(mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0-505043", "", scheme))
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{1: sub},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main, sub.Name: sub},
	}
	reg := &fakeRegistry{present: map[string]bool{
		"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag: true,
	}}
	r := &Reconciler{Scheme: scheme, Registry: reg}

	done, err := r.allOwnedPodsHibernated(t.Context(), cs, classified)
	if err != nil {
		t.Fatalf("allOwnedPodsHibernated: %v", err)
	}
	if done {
		t.Error("must stay pending while sub-agent snapshot is missing")
	}

	reg.present["vk-ns-demo-1-d239dc:"+meta.HibernateSnapshotTag] = true
	done, err = r.allOwnedPodsHibernated(t.Context(), cs, classified)
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
	main := lifecycleHibernated(mustBuildAgentPod(t, cs, 0, "", "", scheme))
	tb := mustBuildToolboxPod(t, cs, cs.Spec.Toolboxes[0], scheme)
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{"tb": tb},
		allByName: map[string]*corev1.Pod{main.Name: main, tb.Name: tb},
	}
	reg := &fakeRegistry{present: map[string]bool{
		"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag: true,
	}}
	r := &Reconciler{Scheme: scheme, Registry: reg}

	done, err := r.allOwnedPodsHibernated(t.Context(), cs, classified)
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
	main := lifecycleHibernated(mustBuildAgentPod(t, cs, 0, "", "", scheme))
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main},
	}
	r := &Reconciler{Scheme: scheme, Registry: &fakeRegistry{probeErr: errors.New("transport boom")}}
	if _, err := r.allOwnedPodsHibernated(t.Context(), cs, classified); err == nil {
		t.Fatal("expected probe error to surface")
	}
}

func TestAllOwnedPodsHibernatedIgnoresStaleTag(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	main := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main},
	}
	reg := &fakeRegistry{present: map[string]bool{
		"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag: true,
	}}
	r := &Reconciler{Scheme: scheme, Registry: reg}

	done, err := r.allOwnedPodsHibernated(t.Context(), cs, classified)
	if err != nil {
		t.Fatalf("allOwnedPodsHibernated: %v", err)
	}
	if done {
		t.Error("stale tag without lifecycle-state=hibernated must not complete the suspend")
	}

	lifecycleHibernated(main)
	done, err = r.allOwnedPodsHibernated(t.Context(), cs, classified)
	if err != nil {
		t.Fatalf("allOwnedPodsHibernated after lifecycle flip: %v", err)
	}
	if !done {
		t.Error("must complete once vk reports hibernated and the tag is present")
	}

	cs.Generation = 5
	done, err = r.allOwnedPodsHibernated(t.Context(), cs, classified)
	if err != nil {
		t.Fatalf("allOwnedPodsHibernated after generation bump: %v", err)
	}
	if done {
		t.Error("lifecycle annotations from a prior round must not complete the current one")
	}
}

func TestAllOwnedPodsHibernatedSkipsTerminalPod(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})
	main := lifecycleHibernated(mustBuildAgentPod(t, cs, 0, "", "", scheme))
	sub := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0-505043", "", scheme)
	sub.Status.Phase = corev1.PodFailed
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{1: sub},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main, sub.Name: sub},
	}
	reg := &fakeRegistry{present: map[string]bool{
		"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag: true,
	}}
	r := &Reconciler{Scheme: scheme, Registry: reg}

	done, err := r.allOwnedPodsHibernated(t.Context(), cs, classified)
	if err != nil {
		t.Fatalf("allOwnedPodsHibernated: %v", err)
	}
	if !done {
		t.Error("terminal sub-agent must be skipped, not awaited forever")
	}
}

func TestEnsureSubAgentsReplacesTerminalPod(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})

	subPod := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0-505043", "", scheme)
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

	changed, _, err := r.ensureSubAgents(t.Context(), cs, classified, "vk-ns-demo-0-505043", "", r.newRestoreIntent(t.Context(), cs.Namespace))
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

func TestEnsureSubAgentsDeadLetterStaysUntilSpecEdit(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})
	subPod := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0-505043", "", scheme)
	subPod.Annotations[annotationDeadLetter] = "0"

	enc, err := encodeRebuildHistory(cs, map[string]rebuildEntry{subPod.Name: {Count: maxRebuildAttempts}})
	if err != nil {
		t.Fatalf("encodeRebuildHistory: %v", err)
	}
	cs.Annotations = map[string]string{annotationRebuildHistory: enc}

	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs, subPod).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{1: subPod},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{subPod.Name: subPod},
	}

	changed, _, err := r.ensureSubAgents(t.Context(), cs, classified, "vk-ns-demo-0-505043", "", r.newRestoreIntent(t.Context(), cs.Namespace))
	if err != nil {
		t.Fatalf("ensureSubAgents: %v", err)
	}
	if changed {
		t.Fatal("dead-lettered pod with matching spec must be left alone")
	}

	cs.Spec.Agent.Image = "ghcr.io/cocoonstack/cocoon/ubuntu:26.04"
	cs.Generation = 1
	changed, _, err = r.ensureSubAgents(t.Context(), cs, classified, "vk-ns-demo-0-505043", "", r.newRestoreIntent(t.Context(), cs.Namespace))
	if err != nil {
		t.Fatalf("ensureSubAgents after spec fix: %v", err)
	}
	if !changed {
		t.Fatal("a spec edit must rebuild a dead-lettered pod")
	}
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: subPod.Namespace, Name: subPod.Name}, &corev1.Pod{}); err == nil {
		t.Error("dead-lettered drifted pod should have been deleted")
	}
	out := mustGetCS(t, cli)
	if _, ok := readRebuildHistory(&out)[subPod.Name]; ok {
		t.Error("rebuild history for the slot must be reset so the new spec gets a fresh budget")
	}
}

func TestEnsureSubAgentsDropsTheOwedSnapshotOfASlotItRecreates(t *testing.T) {
	vm1 := slotNames([]int32{1}, "")[0]
	vm1Tag := vm1 + ":" + meta.HibernateSnapshotTag
	for _, tc := range []struct {
		name         string
		restoredByCR bool
		probeErr     error
		wantDropped  bool
		wantErr      bool
	}{
		{name: "rebuilt slot", wantDropped: true},
		{name: "slot a CocoonHibernation restores", restoredByCR: true},
		{name: "registry refuses", probeErr: errors.New("registry down"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := reclaimOwedSet(t, 1)
			cs.Spec.Agent.Replicas = 1
			objs := []client.Object{cs}
			if tc.restoredByCR {
				objs = append(objs, &cocoonv1.CocoonHibernation{
					ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "ns"},
					Spec: cocoonv1.CocoonHibernationSpec{
						Desire: cocoonv1.HibernationDesireHibernate,
						PodRef: cocoonv1.HibernationPodRef{Name: "demo-1"},
					},
					Status: cocoonv1.CocoonHibernationStatus{Phase: cocoonv1.CocoonHibernationPhaseHibernated},
				})
			}
			reg := &fakeRegistry{probeErr: tc.probeErr, present: map[string]bool{vm1Tag: true}}
			cli := relClient(t, objs...)
			r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

			_, _, err := r.ensureSubAgents(t.Context(), cs, classifyPods(nil), relVMName, "", r.newRestoreIntent(t.Context(), cs.Namespace))
			if (err != nil) != tc.wantErr {
				t.Fatalf("ensureSubAgents error %v, want error %v", err, tc.wantErr)
			}
			if dropped := slices.Contains(reg.deleted, vm1Tag); dropped != tc.wantDropped {
				t.Errorf("%s dropped = %v, want %v", vm1Tag, dropped, tc.wantDropped)
			}
			if owed := slices.Contains(readHibernateReclaim(new(mustGetCS(t, cli))).VMs, vm1); owed == tc.wantDropped {
				t.Errorf("%s still owed = %v, want %v", vm1, owed, !tc.wantDropped)
			}
			var sub corev1.Pod
			getErr := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-1"}, &sub)
			if created := getErr == nil; created == tc.wantErr {
				t.Fatalf("sub-agent created = %v, want %v", created, !tc.wantErr)
			}
			if restores := meta.ReadRestoreFromHibernate(&sub); !tc.wantErr && restores != tc.restoredByCR {
				t.Errorf("sub-agent restores from :hibernate = %v, want %v", restores, tc.restoredByCR)
			}
		})
	}
}

func TestEnsureSubAgentsDropsTheOwedSnapshotOfASlotThatReturnsAfterAScaleDown(t *testing.T) {
	vm2Tag := slotNames([]int32{2}, ":"+meta.HibernateSnapshotTag)[0]
	cs := reclaimOwedSet(t, 2)
	cs.Spec.Agent.Replicas = 2
	sub1 := mustBuildAgentPod(t, cs, 1, relVMName, "", testScheme(t))
	sub2 := mustBuildAgentPod(t, cs, 2, relVMName, "", testScheme(t))
	cs.Spec.Agent.Replicas = 1
	reg := &fakeRegistry{present: map[string]bool{vm2Tag: true}}
	cli := relClient(t, cs, sub1, sub2)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	if _, _, err := r.ensureSubAgents(t.Context(), cs, classifyPods([]corev1.Pod{*sub1, *sub2}), relVMName, "", r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
		t.Fatalf("scale-down: %v", err)
	}
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(sub2), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the extra slot must be deleted, got err=%v", err)
	}

	scaledUp := mustGetCS(t, cli)
	scaledUp.Spec.Agent.Replicas = 2
	if _, _, err := r.ensureSubAgents(t.Context(), &scaledUp, classifyPods([]corev1.Pod{*sub1}), relVMName, "", r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
		t.Fatalf("scale-up: %v", err)
	}
	if !slices.Contains(reg.deleted, vm2Tag) {
		t.Errorf("the returning slot's owed %s must be dropped before its create, deleted %v", vm2Tag, reg.deleted)
	}
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(sub2), &corev1.Pod{}); err != nil {
		t.Errorf("the returning slot must be recreated: %v", err)
	}
	if _, owed := mustGetCS(t, cli).Annotations[annotationHibernateReclaim]; owed {
		t.Errorf("the dropped VM must leave the reclaim record")
	}
}

func TestEnsureSubAgentsRestoresASlotOwedARestore(t *testing.T) {
	vm1 := slotNames([]int32{1}, "")[0]
	vm1Tag := vm1 + ":" + meta.HibernateSnapshotTag
	cs := reclaimOwedSet(t, 1)
	cs.Spec.Agent.Replicas = 1
	cs.Annotations[annotationHibernateReclaim] = encodeReclaim(t, hibernateReclaim{Generation: 2, VMs: []string{vm1}, Restore: []string{vm1}})
	reg := &fakeRegistry{present: map[string]bool{vm1Tag: true}}
	cli := relClient(t, cs)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	if _, _, err := r.ensureSubAgents(t.Context(), cs, classifyPods(nil), relVMName, "node-a", r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
		t.Fatalf("ensureSubAgents: %v", err)
	}
	var sub corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-1"}, &sub); err != nil {
		t.Fatalf("the slot must be created: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&sub) {
		t.Error("a slot hibernated at suspend must restore its snapshot, not fork fresh")
	}
	if slices.Contains(reg.deleted, vm1Tag) {
		t.Errorf("the snapshot the slot restores from must be kept, deleted %v", reg.deleted)
	}
	if owed := readHibernateReclaim(new(mustGetCS(t, cli))); !slices.Contains(owed.VMs, vm1) {
		t.Errorf("the restored VM must stay owed until it wakes, record %+v", owed)
	}
}

func TestEnsureSubAgentsForksAMissingSlotFreshOverASnapshotFromAnotherImage(t *testing.T) {
	const oldImage, newImage = "ghcr.io/cocoonstack/cocoon/ubuntu:24.04", "ghcr.io/cocoonstack/cocoon/ubuntu:26.04"
	vm1 := slotNames([]int32{1}, "")[0]
	vm1Tag := vm1 + ":" + meta.HibernateSnapshotTag
	for _, tc := range []struct {
		name        string
		mainImage   string
		subImage    string
		wantRestore bool
	}{
		{name: "image edited at unsuspend", mainImage: oldImage, subImage: oldImage},
		{name: "sub-agent hibernated on the old image beside a new-image main", mainImage: newImage, subImage: oldImage},
		{name: "snapshot of the spec image", mainImage: newImage, subImage: newImage, wantRestore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
				cs.Generation = 2
				cs.Spec.Agent.Replicas = 1
				cs.Spec.Agent.Image = tc.mainImage
				cs.Annotations = map[string]string{annotationHibernateReclaim: encodeReclaim(t, hibernateReclaim{VMs: slotNames([]int32{0, 1}, ""), Suspended: true})}
			})
			main := rehibernated(mustBuildAgentPod(t, cs, 0, "", "", testScheme(t)))
			cs.Spec.Agent.Image = newImage
			reg := &fakeRegistry{present: map[string]bool{vm1Tag: true}, images: map[string]string{vm1Tag: tc.subImage}}
			cli := relClient(t, cs, main)
			r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

			if err := r.applyUnsuspend(t.Context(), cs, singlePod(main)); err != nil {
				t.Fatalf("applyUnsuspend: %v", err)
			}
			if _, _, err := r.ensureSubAgents(t.Context(), cs, singlePod(main), relVMName, "", r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
				t.Fatalf("ensureSubAgents: %v", err)
			}
			var sub corev1.Pod
			if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-1"}, &sub); err != nil {
				t.Fatalf("the slot must be created: %v", err)
			}
			if restores := meta.ReadRestoreFromHibernate(&sub); restores != tc.wantRestore {
				t.Errorf("sub-agent restores from :hibernate = %v, want %v", restores, tc.wantRestore)
			}
			if dropped := slices.Contains(reg.deleted, vm1Tag); dropped == tc.wantRestore {
				t.Errorf("%s dropped = %v, want %v", vm1Tag, dropped, !tc.wantRestore)
			}
			owed := readHibernateReclaim(new(mustGetCS(t, cli)))
			if slices.Contains(owed.VMs, vm1) != tc.wantRestore || slices.Contains(owed.Restore, vm1) != tc.wantRestore {
				t.Errorf("record %+v, want %s owed a restore = %v", owed, vm1, tc.wantRestore)
			}
		})
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

func TestReconcileMainLifecycleFailedTransitionsToFailed(t *testing.T) {
	cli, cs, _, r := newFailedMainSet(t, false)

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	out := mustGetCS(t, cli)
	if out.Status.Phase != cocoonv1.CocoonSetPhaseFailed {
		t.Errorf("CocoonSet phase = %q, want Failed", out.Status.Phase)
	}
}

func TestReconcileMainLifecycleFailedHonorsSuspend(t *testing.T) {
	cli, cs, mainPod, r := newFailedMainSet(t, true)

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	out := mustGetCS(t, cli)
	if out.Status.Phase != cocoonv1.CocoonSetPhaseSuspending {
		t.Errorf("CocoonSet phase = %q, want Suspending until the failed main's VM is hibernated", out.Status.Phase)
	}
	var pod corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: mainPod.Namespace, Name: mainPod.Name}, &pod); err != nil {
		t.Fatalf("get main pod: %v", err)
	}
	if !meta.ReadHibernateState(&pod) {
		t.Error("failed main must receive the hibernate intent; its VM may still be live")
	}
}

func TestReconcileUnsuspendClearsTheHibernateIntentOfAFailedMain(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
		cs.Generation = 2
		cs.Spec.Agent.OS = cocoonv1.OSMacos
		cs.Status.Phase = cocoonv1.CocoonSetPhaseFailed
	})
	main := rehibernated(mustBuildAgentPod(t, cs, 0, "", "", scheme))
	main.Status.Phase = corev1.PodRunning
	meta.LifecycleStatus{State: meta.LifecycleStateFailed, ObservedGeneration: 2, Message: "macOS guest does not support hibernate"}.Apply(main)
	cli := relClient(t, cs, main)
	rec := record.NewFakeRecorder(8)
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}, Recorder: rec}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("unsuspend pass: %v", err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(main), &got); err != nil {
		t.Fatalf("get main: %v", err)
	}
	if meta.ReadHibernateState(&got) {
		t.Fatal("an unsuspend must clear the hibernate intent of a failed main so its next update carries hibernate=false")
	}
	if phase := mustGetCS(t, cli).Status.Phase; phase != cocoonv1.CocoonSetPhaseFailed {
		t.Errorf("phase = %q, want Failed until vk-cocoon reports the main ready", phase)
	}

	meta.LifecycleStatus{State: meta.LifecycleStateReady, ObservedGeneration: 2}.Apply(&got)
	meta.VMRuntime{VMID: "vm-macos"}.Apply(&got)
	if err := cli.Update(t.Context(), &got); err != nil {
		t.Fatalf("mark main ready: %v", err)
	}
	got.Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := cli.Status().Update(t.Context(), readyPod(&got)); err != nil {
		t.Fatalf("mark main running: %v", err)
	}
	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("recovery pass: %v", err)
	}
	if phase := mustGetCS(t, cli).Status.Phase; phase != cocoonv1.CocoonSetPhaseRunning {
		t.Errorf("phase = %q, want Running once the main is ready again", phase)
	}
	if events := strings.Join(drainEvents(rec), "\n"); !strings.Contains(events, "RecoveredFromFailure") {
		t.Errorf("events = %q, want RecoveredFromFailure", events)
	}
}

func TestReconcileFailedMainKeepsTheQuiesceOfAPendingMigration(t *testing.T) {
	cs := migCocoonSet("node-b")
	cs.Finalizers = []string{finalizerName}
	cs.Generation = 2
	cs.Spec.Agent.OS = cocoonv1.OSMacos
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating
	main := rehibernated(migMainPod(t, cs, "node-a", "vm-macos", true))
	meta.LifecycleStatus{State: meta.LifecycleStateFailed, ObservedGeneration: 2, Message: "macOS guest does not support hibernate"}.Apply(main)
	cli := relClient(t, cs, main)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(main), &got); err != nil {
		t.Fatalf("get main: %v", err)
	}
	if !meta.ReadHibernateState(&got) {
		t.Fatal("the failed path must keep the migration's quiesce, or vk-cocoon lifts the failure and the migration restarts forever")
	}
	gotCS := mustGetCS(t, cli)
	if gotCS.Status.Phase != cocoonv1.CocoonSetPhaseFailed {
		t.Errorf("phase = %q, want Failed", gotCS.Status.Phase)
	}
	if _, owed := gotCS.Annotations[annotationHibernateReclaim]; owed {
		t.Error("a migration's quiesce owes no unsuspend reclaim")
	}
}

func TestReconcileSuspendTimesOutAfterTheDeadline(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
		cs.Spec.Suspend = true
		cs.Annotations = map[string]string{annotationSuspendingSince: time.Now().Add(-suspendTimeout - time.Minute).UTC().Format(time.RFC3339)}
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspending
	})
	mainPod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	mainPod.Status.Phase = corev1.PodRunning
	mainPod.Annotations[meta.AnnotationLifecycleState] = string(meta.LifecycleStateFailed)
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs, mainPod).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	rec := record.NewFakeRecorder(4)
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}, Recorder: rec}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	out := mustGetCS(t, cli)
	if out.Status.Phase != cocoonv1.CocoonSetPhaseFailed {
		t.Fatalf("phase = %q, want Failed after %s in Suspending", out.Status.Phase, suspendTimeout)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "SuspendTimedOut") {
			t.Fatalf("event = %q, want SuspendTimedOut", ev)
		}
	default:
		t.Fatal("a timed-out suspend must raise a SuspendTimedOut event")
	}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	out = mustGetCS(t, cli)
	since, err := time.Parse(time.RFC3339, out.Annotations[annotationSuspendingSince])
	if out.Status.Phase != cocoonv1.CocoonSetPhaseSuspending || err != nil || time.Since(since) > time.Minute {
		t.Fatalf("phase = %q since = %q (%v), want Suspending again with a fresh deadline", out.Status.Phase, out.Annotations[annotationSuspendingSince], err)
	}
}

func TestReconcileSuspendTimesOutWhenTheRegistryProbeKeepsFailing(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
		cs.Spec.Suspend = true
		cs.Annotations = map[string]string{annotationSuspendingSince: time.Now().Add(-suspendTimeout - time.Minute).UTC().Format(time.RFC3339)}
		cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspending
	})
	mainPod := lifecycleHibernated(mustBuildAgentPod(t, cs, 0, "", "", scheme))
	mainPod.Status.Phase = corev1.PodRunning
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs, mainPod).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	rec := record.NewFakeRecorder(4)
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{probeErr: errors.New("registry down")}, Recorder: rec}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile past the deadline must report Failed instead of returning the probe error: %v", err)
	}
	out := mustGetCS(t, cli)
	if out.Status.Phase != cocoonv1.CocoonSetPhaseFailed {
		t.Fatalf("phase = %q, want Failed", out.Status.Phase)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "SuspendTimedOut") || !strings.Contains(ev, "registry down") {
			t.Fatalf("event = %q, want SuspendTimedOut carrying the probe error", ev)
		}
	default:
		t.Fatal("a timed-out suspend must raise a SuspendTimedOut event even while the registry probe fails")
	}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err == nil {
		t.Fatal("inside a fresh deadline the probe error must still be returned")
	}
	out = mustGetCS(t, cli)
	if out.Status.Phase != cocoonv1.CocoonSetPhaseSuspending {
		t.Fatalf("phase = %q after the retry, want Suspending so the next deadline can fire", out.Status.Phase)
	}
	out.Annotations[annotationSuspendingSince] = time.Now().Add(-suspendTimeout - time.Minute).UTC().Format(time.RFC3339)
	if err := cli.Update(t.Context(), &out); err != nil {
		t.Fatalf("age the deadline: %v", err)
	}
	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("second expiry: %v", err)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "SuspendTimedOut") {
			t.Fatalf("event = %q, want a second SuspendTimedOut", ev)
		}
	default:
		t.Fatal("the timeout must fire again once the retried deadline expires")
	}
}

func TestReconcileMainLifecycleFailedWithDriftRecreatesPod(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
	})
	mainPod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	mainPod.Status.Phase = corev1.PodRunning
	mainPod.Annotations[meta.AnnotationLifecycleState] = string(meta.LifecycleStateFailed)

	cs.Spec.Agent.Image = "ghcr.io/cocoonstack/cocoon/ubuntu:26.04"

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, mainPod).
		WithStatusSubresource(&cocoonv1.CocoonSet{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: mainPod.Namespace, Name: mainPod.Name}, &corev1.Pod{}); err == nil {
		t.Error("Failed main pod with drifted spec should have been deleted for recreate")
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("get main pod: %v", err)
	}
}

func TestEnsureSubAgentsTreatsLifecycleFailedAsTerminal(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})
	subPod := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0-505043", "", scheme)
	subPod.Status.Phase = corev1.PodRunning
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

	changed, _, err := r.ensureSubAgents(t.Context(), cs, classified, "vk-ns-demo-0-505043", "", r.newRestoreIntent(t.Context(), cs.Namespace))
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
		WithObjects(cs, tbPod).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{"tb": tbPod},
		allByName: map[string]*corev1.Pod{tbPod.Name: tbPod},
	}

	changed, _, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
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
				{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0-505043"},
			},
			want: []string{
				"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-0-505043:" + meta.DefaultSnapshotTag,
			},
		},
		{
			name:   "always preserves :latest for downstream retag",
			policy: cocoonv1.SnapshotPolicyAlways,
			agents: []cocoonv1.AgentStatus{
				{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0-505043"},
			},
			want: []string{"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag},
		},
		{
			name:   "main-only keeps slot 0, drops other slots and toolboxes",
			policy: cocoonv1.SnapshotPolicyMainOnly,
			agents: []cocoonv1.AgentStatus{
				{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0-505043"},
				{Slot: 1, Role: "sub", PodName: "demo-1", VMName: "vk-ns-demo-1-d239dc"},
			},
			toolboxes: []cocoonv1.ToolboxStatus{
				{Name: "tb", PodName: "demo-tb", VMName: "vk-ns-demo-tb-7f9787"},
			},
			want: []string{
				"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-1-d239dc:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-1-d239dc:" + meta.DefaultSnapshotTag,
				"vk-ns-demo-tb-7f9787:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-tb-7f9787:" + meta.DefaultSnapshotTag,
			},
		},
		{
			name:   "main-only reclaims :latest for toolbox with numeric-suffix name",
			policy: cocoonv1.SnapshotPolicyMainOnly,
			agents: []cocoonv1.AgentStatus{
				{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0-505043"},
			},
			toolboxes: []cocoonv1.ToolboxStatus{
				{Name: "db-0", PodName: "demo-db-0", VMName: "vk-ns-demo-db-0-5b329f"},
			},
			want: []string{
				"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-db-0-5b329f:" + meta.HibernateSnapshotTag,
				"vk-ns-demo-db-0-5b329f:" + meta.DefaultSnapshotTag,
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
			present := map[string]bool{}
			for _, name := range statusVMNames(cs) {
				present[name+":"+meta.HibernateSnapshotTag] = true
				present[name+":"+meta.DefaultSnapshotTag] = true
			}
			reg := &fakeRegistry{present: present}
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

func TestReconcileDeleteSkipsAbsentSnapshotTags(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	cs.Finalizers = []string{finalizerName}
	cs.Spec.SnapshotPolicy = cocoonv1.SnapshotPolicyNever
	cs.Status.Agents = []cocoonv1.AgentStatus{
		{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0-505043"},
	}

	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs).Build()
	reg := &fakeRegistry{}
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}

	if _, err := r.reconcileDelete(t.Context(), cs); err != nil {
		t.Fatalf("reconcileDelete: %v", err)
	}
	if len(reg.deleted) != 0 {
		t.Errorf("DeleteManifest calls = %v, want none", reg.deleted)
	}
	wantProbed := []string{
		"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag,
		"vk-ns-demo-0-505043:" + meta.DefaultSnapshotTag,
	}
	if !slices.Equal(reg.probed, wantProbed) {
		t.Errorf("HasManifest calls = %v, want %v", reg.probed, wantProbed)
	}
}

func TestReconcileDeleteStashesPodVMNamesEvenWhenStatusIsEmpty(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	cs.Finalizers = []string{finalizerName}

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, mustBuildAgentPod(t, cs, 0, "", "", scheme)).
		Build()
	reg := &fakeRegistry{present: map[string]bool{
		"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag: true,
		"vk-ns-demo-0-505043:" + meta.DefaultSnapshotTag:   true,
	}}
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}

	if res, err := r.reconcileDelete(t.Context(), cs); err != nil || res.RequeueAfter == 0 {
		t.Fatalf("pass 1 must delete the pod and requeue, got res=%v err=%v", res, err)
	}
	if _, err := r.reconcileDelete(t.Context(), cs); err != nil {
		t.Fatalf("pass 2: %v", err)
	}

	want := []string{"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag}
	if !slices.Equal(reg.deleted, want) {
		t.Errorf("DeleteManifest calls = %v, want %v", reg.deleted, want)
	}
}

func TestReconcileDeleteCleansTagsAfterPodsGone(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	cs.Finalizers = []string{finalizerName}
	cs.Status.Agents = []cocoonv1.AgentStatus{
		{Slot: 0, Role: "main", PodName: "demo-0", VMName: "vk-ns-demo-0-505043"},
		{Slot: 1, Role: "sub", PodName: "demo-1", VMName: "vk-ns-demo-1-d239dc"},
	}
	cs.Status.Toolboxes = []cocoonv1.ToolboxStatus{
		{Name: "tb", PodName: "demo-tb", VMName: "vk-ns-demo-tb-7f9787"},
	}

	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs).Build()
	reg := &fakeRegistry{present: map[string]bool{
		"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag:  true,
		"vk-ns-demo-1-d239dc:" + meta.HibernateSnapshotTag:  true,
		"vk-ns-demo-tb-7f9787:" + meta.HibernateSnapshotTag: true,
	}}
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}

	if _, err := r.reconcileDelete(t.Context(), cs); err != nil {
		t.Fatalf("reconcileDelete: %v", err)
	}

	want := []string{
		"vk-ns-demo-0-505043:" + meta.HibernateSnapshotTag,
		"vk-ns-demo-1-d239dc:" + meta.HibernateSnapshotTag,
		"vk-ns-demo-tb-7f9787:" + meta.HibernateSnapshotTag,
	}
	if !slices.Equal(reg.deleted, want) {
		t.Errorf("DeleteManifest calls = %v, want %v", reg.deleted, want)
	}
}

func TestReconcileDeleteReclaimsTheSnapshotOfAPodDeletedWhileSuspended(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
		cs.Generation = 3
		cs.Spec.Suspend = true
		cs.Spec.Agent.Replicas = 1
	})
	main, sub := agentPair(t, cs)
	tags := slotNames([]int32{0, 1}, ":"+meta.HibernateSnapshotTag)
	reg := &fakeRegistry{present: map[string]bool{tags[0]: true, tags[1]: true}}
	cli := relClient(t, cs, lifecycleHibernated(main), lifecycleHibernated(sub))
	r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: reg}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("suspend pass: %v", err)
	}
	if err := cli.Delete(t.Context(), sub); err != nil {
		t.Fatalf("delete sub-agent: %v", err)
	}
	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("suspended pass: %v", err)
	}
	live := mustGetCS(t, cli)
	if len(live.Status.Agents) != 1 {
		t.Fatalf("status agents %+v, want only the main once the sub-agent pod is gone", live.Status.Agents)
	}
	if err := cli.Delete(t.Context(), &live); err != nil {
		t.Fatalf("delete set: %v", err)
	}
	for i := range 2 {
		if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
			t.Fatalf("delete pass %d: %v", i+1, err)
		}
	}
	if !slices.Contains(reg.deleted, tags[1]) {
		t.Errorf("teardown must reclaim %s of the sub-agent deleted while suspended, deleted %v", tags[1], reg.deleted)
	}
}

func TestApplyUnsuspendSkipsPodHibernatedByCR(t *testing.T) {
	scheme := testScheme(t)

	hibernated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"},
	}
	meta.HibernateState(true).Apply(hibernated)

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

	cs := newCocoonSet("demo")
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, hibernated, leftover, hibCR).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      hibernated,
		sub:       map[int32]*corev1.Pod{1: leftover},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{"demo-0": hibernated, "demo-1": leftover},
	}

	if err := r.applyUnsuspend(t.Context(), cs, classified); err != nil {
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

func TestApplyUnsuspendSkipsPodMidHibernateOnReverseDesire(t *testing.T) {
	scheme := testScheme(t)

	hibernated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"},
	}
	meta.HibernateState(true).Apply(hibernated)

	hibCR := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-hib", Namespace: "ns"},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireWake,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
		Status: cocoonv1.CocoonHibernationStatus{
			Phase: cocoonv1.CocoonHibernationPhaseHibernating,
		},
	}

	cs := newCocoonSet("demo")
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, hibernated, hibCR).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		main:      hibernated,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{"demo-0": hibernated},
	}

	if err := r.applyUnsuspend(t.Context(), cs, classified); err != nil {
		t.Fatalf("applyUnsuspend: %v", err)
	}

	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("get demo-0: %v", err)
	}
	if !bool(meta.ReadHibernateState(&got)) {
		t.Errorf("demo-0 is mid-Hibernating under a reverse Wake desire; applyUnsuspend must leave it set")
	}
	if _, owed := mustGetCS(t, cli).Annotations[annotationHibernateReclaim]; owed {
		t.Errorf("a pod the CR reconciler owns must not owe this set a reclaim")
	}
}

func TestSetupWithManagerRejectsInvalidConcurrency(t *testing.T) {
	for _, n := range []int{0, -1} {
		if err := (&Reconciler{Concurrency: n}).SetupWithManager(t.Context(), nil); err == nil {
			t.Errorf("concurrency %d must be rejected", n)
		}
	}
}

func TestReconcileSuspendRequeuesWhileOldMainTerminates(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) { cs.Spec.Suspend = true })
	old := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs, old).Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}
	res, err := r.reconcileSuspend(t.Context(), cs, classifiedPods{allByName: map[string]*corev1.Pod{}})
	if err != nil {
		t.Fatalf("a terminating main must requeue, not error: %v", err)
	}
	if res.RequeueAfter != requeueWaitForMain {
		t.Errorf("RequeueAfter = %s, want %s", res.RequeueAfter, requeueWaitForMain)
	}
}

func TestEnsureSubAgentsStashesRemovedSlotVMName(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) { cs.Spec.Agent.Replicas = 1 })
	sub := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0-505043", "", scheme)
	cs.Spec.Agent.Replicas = 0
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs, sub).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	if _, _, err := r.ensureSubAgents(t.Context(), cs, classifyPods([]corev1.Pod{*sub}), "vk-ns-demo-0-505043", "", r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
		t.Fatalf("ensureSubAgents: %v", err)
	}
	if names := stashedVMNames(t, cli); !slices.Contains(names, "vk-ns-demo-1-d239dc") {
		t.Errorf("delete-vm-names = %v, want vk-ns-demo-1-d239dc stashed for teardown GC", names)
	}
}

func TestEnsureToolboxesStashesRemovedToolboxVMName(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	tb := cocoonv1.ToolboxSpec{Name: "tb", Image: "image", Mode: cocoonv1.ToolboxModeRun}
	pod := mustBuildToolboxPod(t, cs, tb, scheme)
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs, pod).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	if _, _, err := r.ensureToolboxes(t.Context(), cs, classifyPods([]corev1.Pod{*pod}), r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
		t.Fatalf("ensureToolboxes: %v", err)
	}
	if names := stashedVMNames(t, cli); !slices.Contains(names, "vk-ns-demo-tb-7f9787") {
		t.Errorf("delete-vm-names = %v, want vk-ns-demo-tb-7f9787 stashed for teardown GC", names)
	}
}

func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev := <-rec.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func agentPair(t *testing.T, cs *cocoonv1.CocoonSet) (*corev1.Pod, *corev1.Pod) {
	t.Helper()
	main := mustBuildAgentPod(t, cs, 0, "", "", testScheme(t))
	main.Spec.NodeName = "node-a"
	return main, mustBuildAgentPod(t, cs, 1, relVMName, "node-a", testScheme(t))
}

func unsuspend(t *testing.T, cli client.Client, edit func(*cocoonv1.CocoonSet)) {
	t.Helper()
	cs := mustGetCS(t, cli)
	cs.Spec.Suspend = false
	cs.Generation++
	if edit != nil {
		edit(&cs)
	}
	if err := cli.Update(t.Context(), &cs); err != nil {
		t.Fatalf("unsuspend: %v", err)
	}
}

func markWoken(t *testing.T, cli client.Client, name, node string, generation int64) {
	t.Helper()
	var pod corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: name}, &pod); err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	meta.LifecycleStatus{State: meta.LifecycleStateReady, ObservedGeneration: generation}.Apply(&pod)
	meta.VMRuntime{VMID: "vm-" + name}.Apply(&pod)
	pod.Spec.NodeName = node
	if err := cli.Update(t.Context(), &pod); err != nil {
		t.Fatalf("mark %s woken: %v", name, err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := cli.Status().Update(t.Context(), readyPod(&pod)); err != nil {
		t.Fatalf("mark %s ready: %v", name, err)
	}
}

func stashedVMNames(t *testing.T, cli client.Client) []string {
	t.Helper()
	return parseVMNamesAnnotation(mustGetCS(t, cli).Annotations[annotationDeleteVMNames])
}

func newFailedMainSet(t *testing.T, suspend bool) (client.Client, *cocoonv1.CocoonSet, *corev1.Pod, *Reconciler) {
	t.Helper()
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Finalizers = []string{finalizerName}
		cs.Spec.Suspend = suspend
	})
	mainPod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	mainPod.Status.Phase = corev1.PodRunning
	mainPod.Annotations[meta.AnnotationLifecycleState] = string(meta.LifecycleStateFailed)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cs, mainPod).
		WithStatusSubresource(&cocoonv1.CocoonSet{}).
		Build()
	return cli, cs, mainPod, &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}
}

func lifecycleHibernated(p *corev1.Pod) *corev1.Pod {
	meta.LifecycleStatus{
		State:              meta.LifecycleStateHibernated,
		ObservedGeneration: meta.ReadCocoonSetGeneration(p),
	}.Apply(p)
	return p
}

func reclaimOwedSet(t *testing.T, slots ...int32) *cocoonv1.CocoonSet {
	t.Helper()
	raw := encodeReclaim(t, hibernateReclaim{Generation: 2, VMs: slotNames(slots, "")})
	return newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 2
		cs.Annotations = map[string]string{annotationHibernateReclaim: raw}
	})
}

func encodeReclaim(t *testing.T, owed hibernateReclaim) string {
	t.Helper()
	raw, err := json.Marshal(owed)
	if err != nil {
		t.Fatalf("encode owed reclaim: %v", err)
	}
	return string(raw)
}

func readyAt(t *testing.T, slot int32, generation int64) *corev1.Pod {
	t.Helper()
	pod := mustBuildAgentPod(t, newCocoonSet("demo"), slot, "", "", testScheme(t))
	meta.LifecycleStatus{State: meta.LifecycleStateReady, ObservedGeneration: generation}.Apply(pod)
	return pod
}

func wokenPod(t *testing.T, slot int32, generation int64) *corev1.Pod {
	t.Helper()
	pod := readyAt(t, slot, generation)
	meta.VMRuntime{VMID: "vm-woken"}.Apply(pod)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
	}
	return pod
}

func inState(p *corev1.Pod, state meta.LifecycleState) *corev1.Pod {
	meta.LifecycleStatus{State: state, ObservedGeneration: meta.ReadLifecycleObservedGeneration(p)}.Apply(p)
	return p
}

func rehibernated(p *corev1.Pod) *corev1.Pod {
	meta.HibernateState(true).Apply(p)
	return p
}

func slotNames(slots []int32, suffix string) []string {
	var out []string
	for _, s := range slots {
		out = append(out, meta.VMNameForDeployment("ns", "demo", int(s))+suffix)
	}
	return out
}

type fakeRegistry struct {
	present   map[string]bool
	images    map[string]string
	probeErr  error
	deleteErr error
	delay     time.Duration
	block     map[string]chan struct{}
	entered   chan string
	probedMu  sync.Mutex
	probed    []string
	deletedMu sync.Mutex
	deleted   []string
}

func (f *fakeRegistry) GetManifest(ctx context.Context, name, tag string) ([]byte, string, error) {
	present, err := f.HasManifest(ctx, name, tag)
	switch {
	case err != nil:
		return nil, "", err
	case !present:
		return nil, "", fmt.Errorf("get manifest %s:%s: %w", name, tag, commonsnapshot.ErrManifestNotFound)
	}
	f.deletedMu.Lock()
	image := f.images[name+":"+tag]
	f.deletedMu.Unlock()
	raw, err := json.Marshal(manifest.OCIManifest{Annotations: map[string]string{manifest.AnnotationSnapshotBaseImage: image}})
	return raw, manifest.MediaTypeOCIManifest, err
}

func (f *fakeRegistry) HasManifest(_ context.Context, name, tag string) (bool, error) {
	f.probedMu.Lock()
	f.probed = append(f.probed, name+":"+tag)
	f.probedMu.Unlock()
	if f.probeErr != nil {
		return false, f.probeErr
	}
	if ch, ok := f.block[name]; ok {
		if f.entered != nil {
			f.entered <- name
		}
		<-ch
	}
	time.Sleep(f.delay)
	f.deletedMu.Lock()
	defer f.deletedMu.Unlock()
	return f.present[name+":"+tag], nil
}

func (f *fakeRegistry) DeleteManifest(_ context.Context, name, tag string) error {
	f.deletedMu.Lock()
	defer f.deletedMu.Unlock()
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = append(f.deleted, name+":"+tag)
	delete(f.present, name+":"+tag)
	return nil
}
