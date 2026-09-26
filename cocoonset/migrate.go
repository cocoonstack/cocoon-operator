package cocoonset

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/metrics"
	"github.com/cocoonstack/cocoon-operator/podpatch"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

// reconcileMigration never loses live state: the old pod dies only after this controller quiesced it and the snapshot exists.
func (r *Reconciler) reconcileMigration(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, ctrl.Result, error) {
	desired := cs.Spec.NodeName
	migrating := cs.Status.Phase == cocoonv1.CocoonSetPhaseMigrating
	main := classified.main
	if main != nil && pinnedElsewhere(main, desired) {
		log.WithFunc("cocoonset.Reconciler.reconcileMigration").Infof(ctx, "migrate %s/%s: main %s waits for a node it is no longer pinned to, recreating it for %s", cs.Namespace, cs.Name, main.Name, cmp.Or(desired, "any node"))
		if err := r.Delete(ctx, main); err != nil && !apierrors.IsNotFound(err) {
			return true, ctrl.Result{}, fmt.Errorf("migrate: delete stale-pinned main %s/%s: %w", main.Namespace, main.Name, err)
		}
		return true, ctrl.Result{RequeueAfter: requeueAfterWrite}, nil
	}
	if desired == "" && !migrating {
		return false, ctrl.Result{}, nil
	}
	// A non-quiesced main on its target or still unscheduled skips the probe; safe because Migrating persists before the first side effect
	if !migrating && main != nil && !bool(meta.ReadHibernateState(main)) && (main.Spec.NodeName == "" || main.Spec.NodeName == desired) {
		return false, ctrl.Result{}, nil
	}
	// A CR-owned hibernation is never migrated; CR hibernation is the long-lived idle state and that reconciler owns the pod
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
		if !mainOffTarget(cs, main) {
			// Settled, aborted, or fresh create: the normal flow takes it from here
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
	case mainOffTarget(cs, main):
		// A tag this controller never quiesced is a leftover that would roll the VM back; drop it first
		if !meta.ReadHibernateState(main) {
			if !meta.VMLive(main) {
				return false, ctrl.Result{}, nil
			}
			owned, err := r.podsTrackedByHibernationCR(ctx, cs.Namespace)
			if err != nil {
				return true, ctrl.Result{}, fmt.Errorf("migrate: %w", err)
			}
			if _, ok := owned[main.Name]; ok {
				return false, ctrl.Result{}, nil
			}
			logger.Warnf(ctx, "migrate %s/%s: stale hibernate snapshot for %s, dropping it first", cs.Namespace, cs.Name, vmName)
			if err := r.Registry.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
				return true, ctrl.Result{}, fmt.Errorf("migrate: drop stale hibernate snapshot %s: %w", vmName, err)
			}
			return r.markMigrating(ctx, cs, classified)
		}
		// NodeName != "" spares the just-recreated, still unscheduled restore pod from a delete/recreate loop
		logger.Infof(ctx, "migrate %s/%s: snapshot in registry, deleting old pod on %s", cs.Namespace, cs.Name, main.Spec.NodeName)
		if err := podpatch.KeepSnapshotOnDelete(ctx, r.Client, main); err != nil {
			logger.Errorf(ctx, err, "migrate: flag keep-snapshot on %s/%s; the wake will cold-pull", main.Namespace, main.Name)
		}
		if err := r.Delete(ctx, main); err != nil && !apierrors.IsNotFound(err) {
			return true, ctrl.Result{}, fmt.Errorf("migrate: delete old main %s/%s: %w", main.Namespace, main.Name, err)
		}
		return r.markMigrating(ctx, cs, classified)

	case main == nil:
		// Recreating also finishes an aborted migration; never strand the snapshot
		pod, err := buildAgentPod(cs, 0, "", "", r.Scheme)
		if err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: build main: %w", err)
		}
		discarded, err := r.discardImageConflict(ctx, cs, pod)
		if err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: %w", err)
		}
		if discarded {
			return false, ctrl.Result{}, nil
		}
		meta.MarkRestoreFromHibernate(pod)
		if err := r.Create(ctx, pod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// The old pod is still Terminating.
				return true, ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
			}
			return true, ctrl.Result{}, fmt.Errorf("migrate: recreate main on %s: %w", cmp.Or(desired, "any node"), err)
		}
		logger.Infof(ctx, "migrate %s/%s: recreated main on %s (restore-from-hibernate)", cs.Namespace, cs.Name, cmp.Or(desired, "any node"))
		return r.markMigrating(ctx, cs, classified)

	case bool(meta.ReadHibernateState(main)) && (desired == "" || main.Spec.NodeName == desired):
		// Quiesced on the target is a re-target back only mid-migration; otherwise applyUnsuspend owns the unsuspend
		if cs.Status.Phase != cocoonv1.CocoonSetPhaseMigrating {
			return false, ctrl.Result{}, nil
		}
		logger.Infof(ctx, "migrate %s/%s: waking %s in place", cs.Namespace, cs.Name, main.Name)
		if err := podpatch.HibernateState(ctx, r.Client, main, false); err != nil {
			return true, ctrl.Result{}, fmt.Errorf("migrate: clear hibernate on %s/%s: %w", main.Namespace, main.Name, err)
		}
		return r.markMigrating(ctx, cs, classified)

	case !meta.VMLive(main):
		// Without the durable Migrating phase this is a CR wake mid-flight, not a migration: disengage
		if cs.Status.Phase != cocoonv1.CocoonSetPhaseMigrating {
			return false, ctrl.Result{}, nil
		}
		if msg := podUnschedulable(main); msg != "" {
			metrics.MigrateUnschedulableTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
			commonk8s.Eventf(r.Recorder, cs, corev1.EventTypeWarning, "MigrateNoCapacity", "main pod %s unschedulable: %s", main.Name, msg)
		}
		return r.markMigrating(ctx, cs, classified)

	default:
		// Restored with a fresh VMID: drop the snapshot; the next pass settles Running
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

func mainOffTarget(cs *cocoonv1.CocoonSet, main *corev1.Pod) bool {
	return main != nil && cs.Spec.NodeName != "" && main.Spec.NodeName != "" && main.Spec.NodeName != cs.Spec.NodeName
}

func pinnedElsewhere(pod *corev1.Pod, nodeName string) bool {
	aff := pod.Spec.Affinity
	if pod.Spec.NodeName != "" || aff == nil || aff.NodeAffinity == nil || aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	return slices.ContainsFunc(aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, func(term corev1.NodeSelectorTerm) bool {
		return slices.ContainsFunc(term.MatchExpressions, func(req corev1.NodeSelectorRequirement) bool {
			return req.Key == corev1.LabelHostname && !slices.Equal(req.Values, []string{nodeName})
		})
	})
}
