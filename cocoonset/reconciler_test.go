package cocoonset

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
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
			reg := &fakeRegistry{present: map[string]bool{relHibernateTagKey: true}}
			cli := relClient(t, cs, main)
			r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: reg}

			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
				t.Fatalf("unsuspend pass: %v", err)
			}
			if err := cli.Get(t.Context(), client.ObjectKeyFromObject(main), &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Fatalf("the drifted main must be deleted for rebuild, got err=%v", err)
			}
			if dropped := slices.Contains(reg.deleted, relHibernateTagKey); dropped != tc.wantDropped {
				t.Fatalf("%s dropped = %v, want %v", relHibernateTagKey, dropped, tc.wantDropped)
			}
			if _, owed := mustGetCS(t, cli).Annotations[annotationHibernateReclaim]; owed == tc.wantDropped {
				t.Errorf("reclaim record kept = %v, want %v", owed, !tc.wantDropped)
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
		})
	}
}

func TestReconcileKeepsAnImageDriftedMainWhileTheRegistryRefusesItsSnapshotDrop(t *testing.T) {
	cs := reclaimOwedSet(t, 0)
	cs.Finalizers = []string{finalizerName}
	cs.Spec.Agent.Image = "ghcr.io/cocoonstack/cocoon/ubuntu:26.04"
	main := readyAt(t, 0, 1)
	cli := relClient(t, cs, main)
	r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: &fakeRegistry{probeErr: errors.New("registry down")}}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err == nil {
		t.Fatal("a refused snapshot drop must fail the pass so it retries with backoff")
	}
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(main), &corev1.Pod{}); err != nil {
		t.Errorf("the main must survive until its snapshot is dropped: %v", err)
	}
	if got := readHibernateReclaim(new(mustGetCS(t, cli))).VMs; !slices.Equal(got, slotNames([]int32{0}, "")) {
		t.Errorf("still owed %v, want the main kept recorded", got)
	}
}

func TestEnsureToolboxesDropsTheOwedSnapshotBeforeAnImageRebuild(t *testing.T) {
	tb := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:1", Mode: cocoonv1.ToolboxModeRun}
	pod := mustBuildToolboxPod(t, newCocoonSet("demo"), tb, testScheme(t))
	tagKey := meta.VMNameForPod("ns", pod.Name) + ":" + meta.HibernateSnapshotTag
	raw, err := json.Marshal(hibernateReclaim{Generation: 2, VMs: []string{meta.VMNameForPod("ns", pod.Name)}})
	if err != nil {
		t.Fatalf("encode owed reclaim: %v", err)
	}
	tb.Image = "ghcr.io/cocoonstack/cocoon/toolbox:2"
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 2
		cs.Annotations = map[string]string{annotationHibernateReclaim: string(raw)}
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{tb}
	})
	reg := &fakeRegistry{present: map[string]bool{tagKey: true}}
	cli := relClient(t, cs, pod)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	if _, _, err := r.ensureToolboxes(t.Context(), cs, classifyPods([]corev1.Pod{*pod}), r.newRestoreIntent(t.Context(), cs.Namespace)); err != nil {
		t.Fatalf("ensureToolboxes: %v", err)
	}
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the drifted toolbox must be deleted for rebuild, got err=%v", err)
	}
	if !slices.Contains(reg.deleted, tagKey) {
		t.Errorf("the owed %s must be dropped before the rebuild, deleted %v", tagKey, reg.deleted)
	}
	if _, owed := mustGetCS(t, cli).Annotations[annotationHibernateReclaim]; owed {
		t.Errorf("the dropped VM must leave the reclaim record")
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

	_, _, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
	if err == nil {
		t.Fatal("ensureToolboxes should return error on name collision with agent pod")
	}
}

func TestEnsureToolboxesRejectsIntegerName(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{
			{Name: "1", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest"},
		}
	})

	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}
	classified := classifiedPods{
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{},
	}

	_, _, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
	if err == nil {
		t.Fatal("ensureToolboxes must reject a toolbox name that collides with agent slot pod naming")
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

	_, _, err := r.ensureToolboxes(t.Context(), cs, classified, r.newRestoreIntent(t.Context(), cs.Namespace))
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
	raw, err := json.Marshal(hibernateReclaim{Generation: 2, VMs: slotNames(slots, "")})
	if err != nil {
		t.Fatalf("encode owed reclaim: %v", err)
	}
	return newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 2
		cs.Annotations = map[string]string{annotationHibernateReclaim: string(raw)}
	})
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
	probeErr  error
	delay     time.Duration
	block     map[string]chan struct{}
	entered   chan string
	probedMu  sync.Mutex
	probed    []string
	deletedMu sync.Mutex
	deleted   []string
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
	f.deleted = append(f.deleted, name+":"+tag)
	delete(f.present, name+":"+tag)
	return nil
}
