package cocoonset

import (
	"cmp"
	"context"
	"fmt"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

// reconcileMigration drives cross-node migration of the main agent (slot 0):
// quiesce -> snapshot -> recreate on the target with restore-from-hibernate ->
// drop the snapshot; idempotent over durable state, handled=false hands back.
// Never lose live state: the old pod dies only after the snapshot exists AND
// this controller quiesced it; the snapshot drops only once the new VM runs.
func (r *Reconciler) reconcileMigration(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, ctrl.Result, error) {
	desired := cs.Spec.NodeName
	migrating := cs.Status.Phase == cocoonv1.CocoonSetPhaseMigrating
	if r.Registry == nil || (desired == "" && !migrating) {
		return false, ctrl.Result{}, nil
	}
	main := classified.main
	// Steady state of a pinned set: skip the registry probe — safe because the
	// Migrating phase is persisted before the first side effect.
	if !migrating && podSettledOn(main, desired) {
		return false, ctrl.Result{}, nil
	}
	// A CR-owned hibernation quiesced on the target is no migration either;
	// short-circuit before the probe — CR hibernation is the long-lived idle state.
	if main != nil && bool(meta.ReadHibernateState(main)) && (desired == "" || main.Spec.NodeName == desired) {
		hibByCR, err := r.podsHibernatedByCR(ctx, cs.Namespace)
		if err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: %w", err)
		}
		if _, owned := hibByCR[main.Name]; owned {
			return false, ctrl.Result{}, nil
		}
	}
	vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, 0)
	snap, err := r.hasHibernateSnapshot(ctx, vmName)
	if err != nil {
		// handled=true: falling through to the normal flow would clear the
		// hibernate annotation mid-migration or fresh-boot over the snapshot.
		return true, ctrl.Result{}, fmt.Errorf("migrate: %w", err)
	}

	if !snap {
		if desired == "" || main == nil || main.Spec.NodeName == "" || main.Spec.NodeName == desired {
			// Settled / aborted / fresh create: the normal flow takes it from here.
			return false, ctrl.Result{}, nil
		}
		return r.startMigration(ctx, cs, classified, desired)
	}
	return r.advanceMigration(ctx, cs, classified, vmName, desired)
}

// startMigration quiesces the wrong-node main pod so vk pushes the :hibernate
// snapshot; Migrating persists first so the fast-path can trust the phase.
func (r *Reconciler) startMigration(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, desired string) (bool, ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.startMigration")
	main := classified.main
	if !meta.ReadHibernateState(main) {
		logger.Infof(ctx, "migrate %s/%s: %s -> %s, hibernating", cs.Namespace, cs.Name, main.Spec.NodeName, desired)
	}
	if err := r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseMigrating)); err != nil {
		return true, ctrl.Result{}, fmt.Errorf("migrate: patch migrating status %s/%s: %w", cs.Namespace, cs.Name, err)
	}
	if err := commonk8s.PatchHibernateState(ctx, r.Client, main, true); err != nil {
		return true, ctrl.Result{}, fmt.Errorf("migrate: patch hibernate on %s/%s: %w", main.Namespace, main.Name, err)
	}
	return true, ctrl.Result{RequeueAfter: requeueMigratePoll}, nil
}

// advanceMigration steps a migration whose :hibernate snapshot exists through
// teardown -> recreate -> restore-wait -> snapshot drop.
func (r *Reconciler) advanceMigration(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, vmName, desired string) (bool, ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.advanceMigration")
	main := classified.main

	switch {
	case main != nil && desired != "" && main.Spec.NodeName != "" && main.Spec.NodeName != desired:
		// A tag this controller never quiesced is a leftover (suspend/unsuspend
		// never deletes it); restoring it would roll the VM back. Drop it.
		if !meta.ReadHibernateState(main) {
			logger.Warnf(ctx, "migrate %s/%s: stale hibernate snapshot for %s, dropping it first", cs.Namespace, cs.Name, vmName)
			if err := r.Registry.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
				return true, ctrl.Result{}, fmt.Errorf("migrate: drop stale hibernate snapshot %s: %w", vmName, err)
			}
			return r.markMigrating(ctx, cs, classified)
		}
		// Snapshot landed; tear down the old-node pod. NodeName != "" spares the
		// just-recreated restore pod (still unscheduled), else it loops delete/recreate.
		logger.Infof(ctx, "migrate %s/%s: snapshot in registry, deleting old pod on %s", cs.Namespace, cs.Name, main.Spec.NodeName)
		if err := r.Delete(ctx, main); err != nil && !apierrors.IsNotFound(err) {
			return true, ctrl.Result{}, fmt.Errorf("migrate: delete old main %s/%s: %w", main.Namespace, main.Name, err)
		}
		return r.markMigrating(ctx, cs, classified)

	case main == nil:
		// Recreating also finishes an aborted migration — never strand the snapshot.
		pod, err := buildAgentPod(cs, 0, "", "", r.Scheme)
		if err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: build main: %w", err)
		}
		meta.MarkRestoreFromHibernate(pod)
		if err := r.Create(ctx, pod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Old pod still Terminating; wait.
				return true, ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
			}
			return true, ctrl.Result{}, fmt.Errorf("migrate: recreate main on %s: %w", cmp.Or(desired, "any node"), err)
		}
		logger.Infof(ctx, "migrate %s/%s: recreated main on %s (restore-from-hibernate)", cs.Namespace, cs.Name, cmp.Or(desired, "any node"))
		return r.markMigrating(ctx, cs, classified)

	case bool(meta.ReadHibernateState(main)) && (desired == "" || main.Spec.NodeName == desired):
		// Quiesced on the target: a re-target back mid-migration or an unsuspend
		// racing the tag (CR-owned was excluded pre-probe). Wake it in place.
		logger.Infof(ctx, "migrate %s/%s: waking %s in place", cs.Namespace, cs.Name, main.Name)
		if err := commonk8s.PatchHibernateState(ctx, r.Client, main, false); err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: clear hibernate on %s/%s: %w", main.Namespace, main.Name, err)
		}
		return r.markMigrating(ctx, cs, classified)

	case !vmLive(main):
		// Restoring — wait for vk. Without the durable Migrating phase this is
		// a CR wake mid-flight, not a migration: disengage, don't repaint.
		if cs.Status.Phase != cocoonv1.CocoonSetPhaseMigrating {
			return false, ctrl.Result{}, nil
		}
		return r.markMigrating(ctx, cs, classified)

	default:
		// Restored with a fresh VMID: drop the snapshot; next pass settles to Running.
		logger.Infof(ctx, "migrate %s/%s: restored on %s, dropping hibernate snapshot", cs.Namespace, cs.Name, desired)
		if err := r.Registry.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: drop hibernate snapshot %s: %w", vmName, err)
		}
		return r.markMigrating(ctx, cs, classified)
	}
}

func (r *Reconciler) markMigrating(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, ctrl.Result, error) {
	if err := r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseMigrating)); err != nil {
		return true, ctrl.Result{}, fmt.Errorf("migrate: patch migrating status %s/%s: %w", cs.Namespace, cs.Name, err)
	}
	return true, ctrl.Result{RequeueAfter: requeueMigratePoll}, nil
}

// podSettledOn reports the steady state of a pinned CocoonSet: the main pod
// runs on the desired node with a live VM and no pending quiesce.
func podSettledOn(main *corev1.Pod, desired string) bool {
	return main != nil && main.Spec.NodeName == desired &&
		vmLive(main) && !bool(meta.ReadHibernateState(main))
}

// vmLive reports a running container with a vk-assigned VMID. Both checks are
// load-bearing: containerStatuses can momentarily report Running before vk
// pulls the snapshot (same gate as the hibernation wake's vmClonedAndRunning).
func vmLive(pod *corev1.Pod) bool {
	return meta.ParseVMRuntime(pod).VMID != "" && meta.IsContainerRunning(pod)
}
