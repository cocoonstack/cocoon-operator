package cocoonset

import (
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

var migVMName = meta.VMNameForDeployment("ns", "demo", 0)

func TestMigrationNoopWithoutNodeName(t *testing.T) {
	cs := newCocoonSet("demo")
	main := migMainPod(t, cs, "node-a", "vmid-1", true)
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{}}
	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil {
		t.Fatalf("reconcileMigration: %v", err)
	}
	if handled {
		t.Error("no spec.nodeName => migration must not engage")
	}
}

func TestMigrationNoopWhenSettledOnTarget(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-b", "vmid-1", true)
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{}}
	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil {
		t.Fatalf("reconcileMigration: %v", err)
	}
	if handled {
		t.Error("main already on target with no snapshot => not migrating")
	}
}

func TestMigrationStartsHibernateOnWrongNode(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "vmid-1", true)
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: &fakeRegistry{}}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("expected handled migration, handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("get main: %v", err)
	}
	if !meta.ReadHibernateState(&got) {
		t.Error("migration start must set hibernate annotation on the old pod")
	}
}

func TestMigrationDeletesOldPodAfterSnapshotLands(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "vmid-1", true)
	meta.HibernateState(true).Apply(main)
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); !apierrors.IsNotFound(err) {
		t.Errorf("old pod must be deleted, got err=%v", err)
	}
	if len(reg.deleted) != 0 {
		t.Errorf("snapshot dropped too early: %v", reg.deleted)
	}
}

func TestMigrationRecreatesOnTargetWithRestoreAnnotation(t *testing.T) {
	cs := migCocoonSet("node-b")
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("recreated pod not found: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&got) {
		t.Error("recreated pod must carry the restore-from-hibernate annotation")
	}
	na := got.Spec.Affinity
	if na == nil || na.NodeAffinity == nil ||
		na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0] != "node-b" {
		t.Errorf("recreated pod must have hostname affinity to node-b, got %+v", na)
	}
}

func TestReconcileMigrationRecreatesTheMainFreshOverASnapshotFromAnotherImage(t *testing.T) {
	cs := migCocoonSet("node-b")
	cs.Finalizers = []string{finalizerName}
	cs.Spec.Agent.Image = "ghcr.io/cocoonstack/cocoon/ubuntu:26.04"
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating
	tagKey := migVMName + ":" + meta.HibernateSnapshotTag
	reg := &fakeRegistry{present: map[string]bool{tagKey: true}, images: map[string]string{tagKey: "ghcr.io/cocoonstack/cocoon/ubuntu:24.04"}}
	cli := relClient(t, cs)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("the main must be recreated: %v", err)
	}
	if meta.ReadRestoreFromHibernate(&got) {
		t.Error("a main whose snapshot came from another image must boot fresh, since vk-cocoon refuses the restore")
	}
	if !slices.Contains(reg.deleted, tagKey) {
		t.Errorf("the snapshot from another image must be dropped, deleted %v", reg.deleted)
	}
	if na := got.Spec.Affinity; na == nil || na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0] != "node-b" {
		t.Errorf("the fresh main must still target node-b, got %+v", na)
	}
}

func TestMigrationWaitsWhileRestoring(t *testing.T) {
	cs := migCocoonSet("node-b")
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating
	main := migMainPod(t, cs, "node-b", "", false)
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if len(reg.deleted) != 0 {
		t.Errorf("must not drop snapshot before the restored VM has a VMID: %v", reg.deleted)
	}
}

func TestMigrationDropsSnapshotWhenRestored(t *testing.T) {
	cs := migCocoonSet("node-b")
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating
	main := migMainPod(t, cs, "node-b", "vmid-new", true)
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if !slices.Contains(reg.deleted, migVMName+":"+meta.HibernateSnapshotTag) {
		t.Errorf("restored VM must drop its hibernate snapshot, deleted=%v", reg.deleted)
	}
}

func TestMigrationDoesNotDeleteRecreatedRestorePod(t *testing.T) {
	cs := migCocoonSet("node-b")
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating
	main := migMainPod(t, cs, "", "", false)
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Errorf("recreated restore pod must not be deleted while NodeName is empty: %v", err)
	}
}

func TestMigrationProbeErrorIsHandled(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "vmid-1", true)
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{probeErr: errors.New("boom")}}
	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if !handled || err == nil {
		t.Errorf("probe failure must own the reconcile, handled=%v err=%v: falling through would unwind the migration", handled, err)
	}
}

func TestMigrationDropsStaleTagInsteadOfDeletingLivePod(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "vmid-1", true)
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Errorf("live pod must survive a stale tag: %v", err)
	}
	if !slices.Contains(reg.deleted, migVMName+":"+meta.HibernateSnapshotTag) {
		t.Errorf("stale tag must be dropped before migrating, deleted=%v", reg.deleted)
	}
}

func TestMigrationWaitsOutAWakeBeforeDroppingTheTagItReads(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "", false)
	tagKey := migVMName + ":" + meta.HibernateSnapshotTag
	reg := &fakeRegistry{present: map[string]bool{tagKey: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil {
		t.Fatalf("reconcileMigration while waking: %v", err)
	}
	if handled {
		t.Error("a main whose wake is in flight must be left to the normal flow")
	}
	if slices.Contains(reg.deleted, tagKey) {
		t.Fatalf("the in-flight wake still reads %s, deleted=%v", tagKey, reg.deleted)
	}

	meta.VMRuntime{VMID: "vmid-woken"}.Apply(main)
	main.Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if handled, _, err = r.reconcileMigration(t.Context(), cs, classifiedPods{main: main}); err != nil || !handled {
		t.Fatalf("woken main: handled=%v err=%v", handled, err)
	}
	if !slices.Contains(reg.deleted, tagKey) {
		t.Fatalf("once the main runs, the leftover %s must be dropped, deleted=%v", tagKey, reg.deleted)
	}
	if handled, _, err = r.reconcileMigration(t.Context(), cs, classifiedPods{main: main}); err != nil || !handled {
		t.Fatalf("migration start: handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("get main: %v", err)
	}
	if !meta.ReadHibernateState(&got) {
		t.Error("with the tag gone the migration must quiesce the main")
	}
}

func TestMigrationLeavesTheSnapshotAloneWhileACocoonHibernationWakes(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "vmid-1", true)
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireWake,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
		Status: cocoonv1.CocoonHibernationStatus{
			Phase:  cocoonv1.CocoonHibernationPhaseWaking,
			VMName: migVMName,
		},
	}
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main, hib).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil {
		t.Fatalf("reconcileMigration: %v", err)
	}
	if handled {
		t.Error("a pod a CocoonHibernation is waking must be left to that reconciler")
	}
	if slices.Contains(reg.deleted, migVMName+":"+meta.HibernateSnapshotTag) {
		t.Fatalf("the pending wake still needs the hibernate snapshot, deleted=%v", reg.deleted)
	}
}

func TestMigrationProceedsOnceTheHibernationWoke(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "vmid-1", true)
	hib := migHibernation(cocoonv1.HibernationDesireWake, cocoonv1.CocoonHibernationPhaseActive)
	hib.Status.ObservedGeneration = hib.Generation
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main, hib).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("a woken hibernation must not pin the pod, handled=%v err=%v", handled, err)
	}
	if !slices.Contains(reg.deleted, migVMName+":"+meta.HibernateSnapshotTag) {
		t.Errorf("the leftover tag must be dropped, deleted=%v", reg.deleted)
	}
}

func TestMigrationLeavesTheSnapshotAloneWhileAFailedWakeRetries(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "", false)
	hib := migHibernation(cocoonv1.HibernationDesireWake, cocoonv1.CocoonHibernationPhaseFailed)
	hib.Status.ObservedGeneration = hib.Generation
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main, hib).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil {
		t.Fatalf("reconcileMigration: %v", err)
	}
	if handled {
		t.Error("a timed-out wake retries on the next reconcile; the pod stays with that reconciler")
	}
	if slices.Contains(reg.deleted, migVMName+":"+meta.HibernateSnapshotTag) {
		t.Fatalf("the wake retry still needs the hibernate snapshot, deleted=%v", reg.deleted)
	}
}

func TestMigrationWaitsWhileAHibernationSpecIsUnobserved(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "vmid-1", true)
	hib := migHibernation(cocoonv1.HibernationDesireWake, cocoonv1.CocoonHibernationPhaseActive)
	hib.Generation = 2
	hib.Status.ObservedGeneration = 1
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main, hib).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil {
		t.Fatalf("reconcileMigration: %v", err)
	}
	if handled {
		t.Error("an unobserved hibernation spec change must be left to that reconciler")
	}
	if slices.Contains(reg.deleted, migVMName+":"+meta.HibernateSnapshotTag) {
		t.Fatalf("the pending wake may still need the snapshot, deleted=%v", reg.deleted)
	}
}

func TestMigrationWakesInPlaceOnRetargetBack(t *testing.T) {
	cs := migCocoonSet("node-b")
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating
	main := migMainPod(t, cs, "node-b", "", false)
	meta.HibernateState(true).Apply(main)
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("get main: %v", err)
	}
	if meta.ReadHibernateState(&got) {
		t.Error("re-target back must wake the pod in place, not deadlock waiting for a restore")
	}
	if len(reg.deleted) != 0 {
		t.Errorf("tag must survive until the VM runs again: %v", reg.deleted)
	}
}

func TestReconcilePlainUnsuspendOfAPinnedSetRestoresASubAgentDeletedWhileSuspended(t *testing.T) {
	vm1 := slotNames([]int32{1}, "")[0]
	cs := migCocoonSet("node-b")
	cs.Finalizers = []string{finalizerName}
	cs.Generation = 2
	cs.Spec.Agent.Replicas = 1
	cs.Status.Phase = cocoonv1.CocoonSetPhaseSuspended
	cs.Annotations = map[string]string{annotationHibernateReclaim: encodeReclaim(t, hibernateReclaim{VMs: slotNames([]int32{0, 1}, ""), Suspended: true})}
	main := rehibernated(migMainPod(t, cs, "node-b", "", false))
	reg := &fakeRegistry{present: map[string]bool{
		migVMName + ":" + meta.HibernateSnapshotTag: true,
		vm1 + ":" + meta.HibernateSnapshotTag:       true,
	}}
	cli := relClient(t, cs, main)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("unsuspend pass: %v", err)
	}
	gotCS := mustGetCS(t, cli)
	if owed := readHibernateReclaim(&gotCS); !slices.Contains(owed.Restore, vm1) {
		t.Fatalf("the unsuspend must record the deleted sub-agent as owed a restore, record %+v", owed)
	}
	if gotCS.Status.Phase == cocoonv1.CocoonSetPhaseMigrating {
		t.Error("a plain unsuspend of a pinned set is not a migration")
	}

	markWoken(t, cli, main.Name, "node-b", 2)
	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("scale pass: %v", err)
	}
	var sub corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-1"}, &sub); err != nil {
		t.Fatalf("the deleted sub-agent must be recreated: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&sub) {
		t.Error("the recreated sub-agent must restore its snapshot, since vk-cocoon refuses a fork-from pod while the tag exists")
	}
}

func TestReconcileUnsuspendIntoANewPinRestoresASubAgentDeletedWhileSuspended(t *testing.T) {
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
	unsuspend(t, cli, func(cs *cocoonv1.CocoonSet) { cs.Spec.NodeName = "node-b" })
	for i := range 2 {
		if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
			t.Fatalf("migration pass %d: %v", i+1, err)
		}
	}
	var moved corev1.Pod
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(main), &moved); err != nil || !meta.ReadRestoreFromHibernate(&moved) {
		t.Fatalf("the migration must recreate the main restore-marked on the target, err=%v", err)
	}
	markWoken(t, cli, main.Name, "node-b", 4)
	for i := range 3 {
		if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
			t.Fatalf("pass %d: %v", i+3, err)
		}
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(sub), &got); err != nil {
		t.Fatalf("the sub-agent must be recreated: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&got) {
		t.Error("the sub-agent must restore its snapshot, since vk-cocoon refuses a fork-from pod while the tag exists")
	}
	if !reg.present[tags[1]] {
		t.Errorf("the snapshot the sub-agent restores from must be kept, deleted %v", reg.deleted)
	}
}

func TestReconcileRepinRecreatesAMainPendingUnderTheOldPin(t *testing.T) {
	for _, tc := range []struct {
		name        string
		phase       cocoonv1.CocoonSetPhase
		restoring   bool
		wantRestore bool
	}{
		{name: "main pending its first boot"},
		{name: "migration restore pending on a full node", phase: cocoonv1.CocoonSetPhaseMigrating, restoring: true, wantRestore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := migCocoonSet("node-a")
			cs.Finalizers = []string{finalizerName}
			cs.Generation = 4
			cs.Status.Phase = tc.phase
			pending := mustBuildAgentPod(t, cs, 0, "", "", testScheme(t))
			if tc.restoring {
				meta.MarkRestoreFromHibernate(pending)
			}
			pending.Status.Conditions = []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "0/2 nodes are available: Insufficient memory.",
			}}
			cs.Spec.NodeName = "node-b"
			cs.Generation = 5
			reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: tc.restoring}}
			cli := relClient(t, cs, pending)
			r := &Reconciler{Client: cli, APIReader: cli, Scheme: testScheme(t), Registry: reg}

			for i := range 2 {
				if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
					t.Fatalf("pass %d: %v", i+1, err)
				}
			}
			var got corev1.Pod
			if err := cli.Get(t.Context(), client.ObjectKeyFromObject(pending), &got); err != nil {
				t.Fatalf("the main must be recreated: %v", err)
			}
			if na := got.Spec.Affinity; na == nil || na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0] != "node-b" {
				t.Errorf("the main must be recreated under the new pin node-b, got %+v", na)
			}
			if restores := meta.ReadRestoreFromHibernate(&got); restores != tc.wantRestore {
				t.Errorf("recreated main restores from :hibernate = %v, want %v", restores, tc.wantRestore)
			}
		})
	}
}

func TestMigrationLeavesCRHibernationAlone(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-b", "", false)
	meta.HibernateState(true).Apply(main)
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "h", Namespace: "ns"},
		Spec: cocoonv1.CocoonHibernationSpec{
			PodRef: cocoonv1.HibernationPodRef{Name: main.Name},
			Desire: cocoonv1.HibernationDesireHibernate,
		},
	}
	reg := &fakeRegistry{probeErr: errors.New("boom")}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main, hib).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil {
		t.Fatalf("reconcileMigration: %v", err)
	}
	if handled {
		t.Error("CR-owned hibernation on the target is not a migration")
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("get main: %v", err)
	}
	if !meta.ReadHibernateState(&got) {
		t.Error("must not wake a CR-hibernated pod")
	}
}

func TestMigrationLeavesCRHibernationAloneOffTarget(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-a", "", false)
	meta.HibernateState(true).Apply(main)
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "h", Namespace: "ns"},
		Spec: cocoonv1.CocoonHibernationSpec{
			PodRef: cocoonv1.HibernationPodRef{Name: main.Name},
			Desire: cocoonv1.HibernationDesireHibernate,
		},
	}
	reg := &fakeRegistry{probeErr: errors.New("boom")}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main, hib).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil {
		t.Fatalf("reconcileMigration: %v", err)
	}
	if handled {
		t.Error("a CR-hibernated main off the target must wait for its CR, not migrate")
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("CR-hibernated main must not be deleted: %v", err)
	}
}

func TestMigrationFinishesAbortedRestore(t *testing.T) {
	cs := migCocoonSet("")
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &got); err != nil {
		t.Fatalf("aborted migration must still recreate the restore pod: %v", err)
	}
	if !meta.ReadRestoreFromHibernate(&got) {
		t.Error("recreated pod must restore from the snapshot, not fresh-boot over it")
	}
	if got.Spec.Affinity != nil {
		t.Errorf("no nodeName => no affinity, got %+v", got.Spec.Affinity)
	}
}

func TestMigrationSkipsProbeWhenSettled(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-b", "vmid-1", true)
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{probeErr: errors.New("boom")}}
	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if handled || err != nil {
		t.Errorf("settled pinned set must skip the probe, handled=%v err=%v", handled, err)
	}
}

func TestMigrationSkipsProbeWhileMainBoots(t *testing.T) {
	for _, node := range []string{"", "node-b"} {
		t.Run("node="+node, func(t *testing.T) {
			cs := migCocoonSet("node-b")
			main := migMainPod(t, cs, node, "", false)
			r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{probeErr: errors.New("boom")}}
			handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
			if handled || err != nil {
				t.Errorf("booting pinned main must skip the probe, handled=%v err=%v", handled, err)
			}
		})
	}
}

func TestMigrationDisengagesFromCRWakeWindow(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-b", "", false)
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	r := &Reconciler{Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil {
		t.Fatalf("reconcileMigration: %v", err)
	}
	if handled {
		t.Error("a CR wake without the Migrating phase must not be repainted as a migration")
	}
	if len(reg.deleted) != 0 {
		t.Errorf("must not touch the wake's snapshot: %v", reg.deleted)
	}
}

func TestMigrationReportsAnUnschedulableTarget(t *testing.T) {
	cs := migCocoonSet("node-b")
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating
	main := migMainPod(t, cs, "node-b", "", false)
	main.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "0/2 nodes are available: Insufficient memory.",
	}}
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	rec := record.NewFakeRecorder(4)
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg, Recorder: rec}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "MigrateNoCapacity") || !strings.Contains(ev, "Insufficient memory") || strings.Contains(ev, "node-b") {
			t.Fatalf("event = %q, want MigrateNoCapacity carrying the scheduler message and naming no target node", ev)
		}
	default:
		t.Fatal("an unschedulable migration target must raise a MigrateNoCapacity event")
	}
	var out cocoonv1.CocoonSet
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: cs.Namespace, Name: cs.Name}, &out); err != nil {
		t.Fatalf("get CocoonSet: %v", err)
	}
	if out.Status.Phase != cocoonv1.CocoonSetPhaseMigrating {
		t.Errorf("phase = %q, want Migrating kept while waiting for capacity", out.Status.Phase)
	}
}

func migHibernation(desire cocoonv1.HibernationDesire, phase cocoonv1.CocoonHibernationPhase) *cocoonv1.CocoonHibernation {
	return &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns", Generation: 1},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: desire,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
		Status: cocoonv1.CocoonHibernationStatus{Phase: phase, VMName: migVMName},
	}
}

func migCocoonSet(node string) *cocoonv1.CocoonSet {
	return newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) { cs.Spec.NodeName = node })
}

func migMainPod(t *testing.T, cs *cocoonv1.CocoonSet, node, vmid string, running bool) *corev1.Pod {
	t.Helper()
	pod := mustBuildAgentPod(t, cs, 0, "", "", testScheme(t))
	pod.Spec.NodeName = node
	if vmid != "" {
		meta.VMRuntime{VMID: vmid}.Apply(pod)
	}
	if running {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{
			{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		}
	}
	return pod
}
