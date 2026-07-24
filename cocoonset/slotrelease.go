package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/metrics"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

// reconcileSuspendRelease drives a suspended release-policy CocoonSet to zero
// pods so its scheduling seat frees; pods are deleted only after every managed
// VM's :hibernate snapshot is verified in the registry.
func (r *Reconciler) reconcileSuspendRelease(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.reconcileSuspendRelease")

	if r.Registry == nil {
		return ctrl.Result{}, fmt.Errorf("hibernatePolicy=release on %s/%s requires a configured registry", cs.Namespace, cs.Name)
	}

	if len(classified.allByName) == 0 {
		// Seat already released, or suspended before first boot — nothing to
		// snapshot; settle Suspended.
		return ctrl.Result{}, r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseSuspended))
	}

	if err := r.applySuspend(ctx, classified); err != nil {
		return ctrl.Result{}, err
	}
	allHibernated, err := r.allOwnedPodsHibernated(ctx, cs, classified)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !allHibernated {
		return ctrl.Result{RequeueAfter: requeueSuspendPoll},
			r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseSuspending))
	}

	// Stash before the first delete: delete-time GC needs the vm names (status
	// rebuilds empty once the pods are gone) and wake needs the node hint.
	if err := r.stashDeleteVMNames(ctx, cs, podsSlice(classified)); err != nil {
		return ctrl.Result{}, fmt.Errorf("stash vm names before slot release: %w", err)
	}
	if main := classified.main; main != nil && main.Spec.NodeName != "" {
		if err := r.patchAnnotation(ctx, cs, meta.AnnotationHibernatedOnNode, main.Spec.NodeName); err != nil {
			return ctrl.Result{}, err
		}
	}

	deleteErr := classified.forEachSorted(ctx, func(pod *corev1.Pod) error {
		if podIsTerminal(pod) {
			// Terminal pods carry no VM state; keep them for post-unsuspend triage.
			return nil
		}
		logger.Infof(ctx, "slot release: deleting hibernated pod %s/%s (node=%s)", pod.Namespace, pod.Name, pod.Spec.NodeName)
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("slot release: delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		metrics.SlotReleasePodsDeletedTotal.WithLabelValues(pod.Namespace, cs.Name).Inc()
		return nil
	})
	if deleteErr != nil {
		return ctrl.Result{}, deleteErr
	}
	// Suspended lands with the deletes; wake's stale-cache arm keys on this receipt.
	return ctrl.Result{RequeueAfter: requeueSuspendPoll},
		r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseSuspended))
}

// reconcileWake owns the reconcile of a set coming back from a pod-less
// Suspended state: durable Waking phase first, restore pod second, tag drop
// only once the VM is live. Gated on phase, not current policy — a policy
// edit made while suspended must not fresh-boot over the snapshot.
func (r *Reconciler) reconcileWake(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.reconcileWake")
	main := classified.main
	hint := cs.Annotations[meta.AnnotationHibernatedOnNode]
	waking := cs.Status.Phase == cocoonv1.CocoonSetPhaseWaking
	suspended := cs.Status.Phase == cocoonv1.CocoonSetPhaseSuspended
	// A fast unsuspend can land before the deletes settle Suspended; the node
	// hint keeps the wake engaged (phase-scoped so a stale hint never fires).
	suspending := cs.Status.Phase == cocoonv1.CocoonSetPhaseSuspending && hint != ""
	// A stale-phase read can overwrite Waking; a restore-marked, non-hibernating
	// main with the hint set is an unfinished wake (completion clears the hint).
	cleanupPending := main != nil && meta.ReadRestoreFromHibernate(main) &&
		!bool(meta.ReadHibernateState(main)) && hint != ""
	waking = waking || cleanupPending
	if (!waking && !suspended && !suspending) || r.Registry == nil {
		return false, ctrl.Result{}, nil
	}

	switch {
	case main == nil:
		return r.startReleasedWake(ctx, cs, classified)

	case waking && !vmLive(main):
		// Restore in flight. Unschedulable is the out-of-stock signal;
		// surface it but keep waiting — a seat may free up.
		if msg := podUnschedulable(main); msg != "" {
			metrics.SlotReleaseWakeUnschedulableTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
			if r.Recorder != nil {
				r.Recorder.Eventf(cs, corev1.EventTypeWarning, "WakeNoCapacity", "main pod %s unschedulable: %s", main.Name, msg)
			}
		}
		return true, ctrl.Result{RequeueAfter: requeueSuspendPoll},
			r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseWaking))

	case waking && vmLive(main):
		vmName := meta.ParseVMSpec(main).VMName
		logger.Infof(ctx, "wake %s/%s: restored on %s, dropping hibernate snapshot", cs.Namespace, cs.Name, main.Spec.NodeName)
		if err := r.Registry.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
			return true, ctrl.Result{}, fmt.Errorf("wake: drop hibernate snapshot %s: %w", vmName, err)
		}
		placement := "pool"
		if hint != "" && hint == main.Spec.NodeName {
			placement = "hint-node"
		}
		metrics.SlotReleaseWakeTotal.WithLabelValues(cs.Namespace, cs.Name, placement).Inc()
		if err := r.patchAnnotation(ctx, cs, meta.AnnotationHibernatedOnNode, ""); err != nil {
			return true, ctrl.Result{}, err
		}
		// Auto-derived phase; the requeued pass settles Running/Scaling.
		return true, ctrl.Result{Requeue: true}, r.patchStatus(ctx, cs, buildStatus(cs, classified, ""))

	case (suspended || suspending) && hint != "" && !podIsTerminal(main):
		// Either a stale view of the deleted main or a delete that never ran;
		// only an uncached read can tell them apart.
		return r.confirmReleasedDelete(ctx, main)

	default:
		// Retain placeholder or kept terminal pod: the normal flow owns these.
		return false, ctrl.Result{}, nil
	}
}

// startReleasedWake probes the registry and recreates the main pod with
// restore intent; handled=false only when there is no snapshot to restore.
func (r *Reconciler) startReleasedWake(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.startReleasedWake")
	vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, 0)
	present, probeErr := snapshot.HasHibernateSnapshot(ctx, r.Registry, vmName)
	if probeErr != nil {
		// Fail closed: falling through would fresh-boot over a real snapshot.
		return true, ctrl.Result{}, fmt.Errorf("wake: %w", probeErr)
	}
	if !present {
		// No snapshot: suspended before first boot; the normal flow fresh-boots.
		return false, ctrl.Result{}, nil
	}
	// Persist Waking before the create so a crash between the two steps
	// resumes here instead of fresh-booting.
	if err := r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseWaking)); err != nil {
		return true, ctrl.Result{}, err
	}
	pod, err := buildAgentPod(cs, 0, "", "", r.Scheme)
	if err != nil {
		return true, ctrl.Result{}, fmt.Errorf("wake: build main: %w", err)
	}
	meta.MarkRestoreFromHibernate(pod)
	// Soft-prefer the hibernated-on seat; a spec.nodeName pin already set
	// a required affinity in buildAgentPod and wins.
	if node := cs.Annotations[meta.AnnotationHibernatedOnNode]; node != "" && pod.Spec.Affinity == nil {
		pod.Spec.Affinity = preferredHostnameAffinity(node)
	}
	if err := r.Create(ctx, pod); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return true, ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
		}
		return true, ctrl.Result{}, fmt.Errorf("wake: create main: %w", err)
	}
	logger.Infof(ctx, "wake %s/%s: recreated main unpinned, preferred_node=%s", cs.Namespace, cs.Name, cs.Annotations[meta.AnnotationHibernatedOnNode])
	return true, ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
}

func (r *Reconciler) confirmReleasedDelete(ctx context.Context, main *corev1.Pod) (bool, ctrl.Result, error) {
	var live corev1.Pod
	err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(main), &live)
	switch {
	case apierrors.IsNotFound(err) || (err == nil && live.DeletionTimestamp != nil):
		return true, ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
	case err != nil:
		return true, ctrl.Result{}, fmt.Errorf("wake: confirm delete of %s/%s: %w", main.Namespace, main.Name, err)
	default:
		return false, ctrl.Result{}, nil
	}
}

func podUnschedulable(pod *corev1.Pod) string {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
			return c.Message
		}
	}
	return ""
}

func podsSlice(c classifiedPods) []corev1.Pod {
	out := make([]corev1.Pod, 0, len(c.allByName))
	for _, name := range slices.Sorted(maps.Keys(c.allByName)) {
		out = append(out, *c.allByName[name])
	}
	return out
}
