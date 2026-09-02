package cocoonset

import (
	"cmp"
	"context"
	"fmt"

	"github.com/cocoonstack/cocoon-operator/podpatch"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

// reconcileMigration never loses live state: the old pod dies only after this controller quiesced it and the snapshot exists.
func (r *Reconciler) reconcileMigration(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, ctrl.Result, error) {
	desired := cs.Spec.NodeName
	migrating := cs.Status.Phase == cocoonv1.CocoonSetPhaseMigrating
	if r.Registry == nil || (desired == "" && !migrating) {
		return false, ctrl.Result{}, nil
	}
	main := classified.main
	// a non-quiesced main on its target or still unscheduled skips the probe; safe because Migrating persists before the first side effect
	if !migrating && main != nil && !bool(meta.ReadHibernateState(main)) && (main.Spec.NodeName == "" || main.Spec.NodeName == desired) {
		return false, ctrl.Result{}, nil
	}
	// a CR-owned hibernation is never migrated; CR hibernation is the long-lived idle state and that reconciler owns the pod
	if main != nil && bool(meta.ReadHibernateState(main)) {
		hibByCR, err := r.podsHibernatedByCR(ctx, cs.Namespace)
		if err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: %w", err)
		}
		if _, owned := hibByCR[main.Name]; owned {
			return false, ctrl.Result{}, nil
		}
	}
	vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, 0)
	snap, err := snapshot.HasHibernateSnapshot(ctx, r.Registry, vmName)
	if err != nil {
		// handled=true: the normal flow would clear the hibernate annotation mid-migration or fresh-boot over the snapshot
		return true, ctrl.Result{}, fmt.Errorf("migrate: %w", err)
	}

	if !snap {
		if desired == "" || main == nil || main.Spec.NodeName == "" || main.Spec.NodeName == desired {
			// settled, aborted, or fresh create: the normal flow takes it from here
			return false, ctrl.Result{}, nil
		}
		return r.startMigration(ctx, cs, classified, desired)
	}
	return r.advanceMigration(ctx, cs, classified, vmName, desired)
}

// startMigration persists Migrating before quiescing so the fast path can trust the phase.
func (r *Reconciler) startMigration(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, desired string) (bool, ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.startMigration")
	main := classified.main
	if !meta.ReadHibernateState(main) {
		logger.Infof(ctx, "migrate %s/%s: %s -> %s, hibernating", cs.Namespace, cs.Name, main.Spec.NodeName, desired)
	}
	if err := r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseMigrating)); err != nil {
		return true, ctrl.Result{}, fmt.Errorf("migrate: patch migrating status %s/%s: %w", cs.Namespace, cs.Name, err)
	}
	if err := podpatch.HibernateState(ctx, r.Client, main, true); err != nil {
		return true, ctrl.Result{}, fmt.Errorf("migrate: patch hibernate on %s/%s: %w", main.Namespace, main.Name, err)
	}
	return true, ctrl.Result{RequeueAfter: requeueMigratePoll}, nil
}

func (r *Reconciler) advanceMigration(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, vmName, desired string) (bool, ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.advanceMigration")
	main := classified.main

	switch {
	case main != nil && desired != "" && main.Spec.NodeName != "" && main.Spec.NodeName != desired:
		// a tag this controller never quiesced is a leftover that would roll the VM back; drop it first
		if !meta.ReadHibernateState(main) {
			logger.Warnf(ctx, "migrate %s/%s: stale hibernate snapshot for %s, dropping it first", cs.Namespace, cs.Name, vmName)
			if err := r.Registry.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
				return true, ctrl.Result{}, fmt.Errorf("migrate: drop stale hibernate snapshot %s: %w", vmName, err)
			}
			return r.markMigrating(ctx, cs, classified)
		}
		// NodeName != "" spares the just-recreated, still unscheduled restore pod from a delete/recreate loop
		logger.Infof(ctx, "migrate %s/%s: snapshot in registry, deleting old pod on %s", cs.Namespace, cs.Name, main.Spec.NodeName)
		if err := r.Delete(ctx, main); err != nil && !apierrors.IsNotFound(err) {
			return true, ctrl.Result{}, fmt.Errorf("migrate: delete old main %s/%s: %w", main.Namespace, main.Name, err)
		}
		return r.markMigrating(ctx, cs, classified)

	case main == nil:
		// recreating also finishes an aborted migration; never strand the snapshot
		pod, err := buildAgentPod(cs, 0, "", "", r.Scheme)
		if err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: build main: %w", err)
		}
		meta.MarkRestoreFromHibernate(pod)
		if err := r.Create(ctx, pod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// old pod still Terminating; wait
				return true, ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
			}
			return true, ctrl.Result{}, fmt.Errorf("migrate: recreate main on %s: %w", cmp.Or(desired, "any node"), err)
		}
		logger.Infof(ctx, "migrate %s/%s: recreated main on %s (restore-from-hibernate)", cs.Namespace, cs.Name, cmp.Or(desired, "any node"))
		return r.markMigrating(ctx, cs, classified)

	case bool(meta.ReadHibernateState(main)) && (desired == "" || main.Spec.NodeName == desired):
		// quiesced on the target: a re-target back or an unsuspend racing the tag
		logger.Infof(ctx, "migrate %s/%s: waking %s in place", cs.Namespace, cs.Name, main.Name)
		if err := podpatch.HibernateState(ctx, r.Client, main, false); err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: clear hibernate on %s/%s: %w", main.Namespace, main.Name, err)
		}
		return r.markMigrating(ctx, cs, classified)

	case !vmLive(main):
		// without the durable Migrating phase this is a CR wake mid-flight, not a migration: disengage
		if cs.Status.Phase != cocoonv1.CocoonSetPhaseMigrating {
			return false, ctrl.Result{}, nil
		}
		return r.markMigrating(ctx, cs, classified)

	default:
		// restored with a fresh VMID: drop the snapshot; the next pass settles Running
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

// vmLive needs both checks: containerStatuses can report Running before vk pulls the snapshot.
func vmLive(pod *corev1.Pod) bool {
	return meta.ParseVMRuntime(pod).VMID != "" && meta.IsContainerRunning(pod)
}
