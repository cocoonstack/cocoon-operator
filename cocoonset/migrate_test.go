package cocoonset

import (
	"errors"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{}} // fast-path: registry never consulted
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
	meta.HibernateState(true).Apply(main) // quiesced by the migration start pass
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
	// Ordering gate: snapshot must NOT be dropped while tearing down the old pod.
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

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{}) // main absent
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

func TestMigrationWaitsWhileRestoring(t *testing.T) {
	cs := migCocoonSet("node-b")
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating // real migrations persist the phase up front
	main := migMainPod(t, cs, "node-b", "", false)     // on target, no VMID yet
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
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating // the settled fast-path defers to the in-flight migration
	main := migMainPod(t, cs, "node-b", "vmid-new", true) // restored on target
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
	// Freshly recreated restore pod: NodeName empty (awaiting scheduling), no VMID.
	main := migMainPod(t, cs, "", "", false)
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs, main).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	// An unscheduled restore pod must survive, else the snapshot branch loops
	// delete/recreate forever.
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
	main := migMainPod(t, cs, "node-a", "vmid-1", true) // live, never quiesced
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

func TestMigrationWakesInPlaceOnRetargetBack(t *testing.T) {
	cs := migCocoonSet("node-b")
	main := migMainPod(t, cs, "node-b", "", false) // quiesced on the (re-)target
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
	// probeErr proves CR-owned hibernation short-circuits before the registry probe.
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

func TestMigrationFinishesAbortedRestore(t *testing.T) {
	cs := migCocoonSet("") // nodeName cleared mid-flight
	cs.Status.Phase = cocoonv1.CocoonSetPhaseMigrating
	reg := &fakeRegistry{present: map[string]bool{migVMName + ":" + meta.HibernateSnapshotTag: true}}
	cli := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cs).WithStatusSubresource(&cocoonv1.CocoonSet{}).Build()
	r := &Reconciler{Client: cli, Scheme: testScheme(t), Registry: reg}

	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{}) // old pod already deleted
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
	// probeErr proves the registry is never consulted in steady state.
	r := &Reconciler{Scheme: testScheme(t), Registry: &fakeRegistry{probeErr: errors.New("boom")}}
	handled, _, err := r.reconcileMigration(t.Context(), cs, classifiedPods{main: main})
	if handled || err != nil {
		t.Errorf("settled pinned set must skip the probe, handled=%v err=%v", handled, err)
	}
}

func TestMigrationDisengagesFromCRWakeWindow(t *testing.T) {
	// CR wake mid-flight: annotation cleared, tag not yet dropped, VM not yet
	// live, no Migrating phase — not a migration.
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
