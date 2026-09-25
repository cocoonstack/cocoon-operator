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
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/metrics"
	"github.com/cocoonstack/cocoon-operator/podpatch"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

const annotationHibernatedImage = "cocoonset.cocoonstack.io/hibernated-image"

// reconcileSuspendRelease drains a release-policy CocoonSet to zero pods once every VM's :hibernate snapshot is verified.
func (r *Reconciler) reconcileSuspendRelease(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.reconcileSuspendRelease")

	if !hasLivePod(classified) {
		// Seat already released, suspended before first boot, or only terminal pods left: settle Suspended
		if err := r.clearSuspendDeadline(ctx, cs); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseSuspended))
	}

	if err := r.applySuspend(ctx, classified); err != nil {
		return ctrl.Result{}, err
	}
	allHibernated, err := r.allOwnedPodsHibernated(ctx, cs, classified)
	if err != nil || !allHibernated {
		return r.pollSuspend(ctx, cs, classified, err)
	}
	if err := r.clearSuspendDeadline(ctx, cs); err != nil {
		return ctrl.Result{}, err
	}

	// Stash before the first delete: GC needs the vm names and wake needs the node hint once the pods are gone
	if err := r.stashDeleteVMNames(ctx, cs, podsSlice(classified)); err != nil {
		return ctrl.Result{}, fmt.Errorf("stash vm names before slot release: %w", err)
	}
	if main := classified.main; main != nil {
		if err := r.patchAnnotation(ctx, cs, annotationHibernatedImage, meta.ParseVMSpec(main).Image); err != nil {
			return ctrl.Result{}, err
		}
		if main.Spec.NodeName != "" {
			if err := r.patchAnnotation(ctx, cs, meta.AnnotationHibernatedOnNode, main.Spec.NodeName); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	deleteErr := classified.forEachSorted(ctx, func(pod *corev1.Pod) error {
		if meta.IsPodTerminal(pod) {
			// kubelet-terminal pods carry no VM state; keep them for post-unsuspend triage
			return nil
		}
		logger.Infof(ctx, "slot release: deleting hibernated pod %s/%s (node=%s)", pod.Namespace, pod.Name, pod.Spec.NodeName)
		// Best-effort: a lost flag costs the wake a registry pull; failing here would forfeit the seat
		if err := podpatch.KeepSnapshotOnDelete(ctx, r.Client, pod); err != nil {
			logger.Errorf(ctx, err, "slot release: flag keep-snapshot on %s/%s; wake will cold-pull", pod.Namespace, pod.Name)
		}
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

// reconcileWake gates on Status.Phase, not policy, so a mid-suspend policy edit cannot fresh-boot over the snapshot.
func (r *Reconciler) reconcileWake(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.reconcileWake")
	main := classified.main
	hint := cs.Annotations[meta.AnnotationHibernatedOnNode]
	waking := cs.Status.Phase == cocoonv1.CocoonSetPhaseWaking
	suspended := cs.Status.Phase == cocoonv1.CocoonSetPhaseSuspended
	// A fast unsuspend can land before the deletes settle Suspended; the phase-scoped node hint keeps the wake engaged
	suspending := cs.Status.Phase == cocoonv1.CocoonSetPhaseSuspending && hint != ""
	// A restore-marked, non-hibernating main with the hint still set is an unfinished wake; completion clears the hint
	cleanupPending := main != nil && meta.ReadRestoreFromHibernate(main) &&
		!bool(meta.ReadHibernateState(main)) && hint != ""
	waking = waking || cleanupPending
	if !waking && !suspended && !suspending {
		return false, ctrl.Result{}, nil
	}

	switch {
	case main == nil:
		return r.startReleasedWake(ctx, cs, classified)

	case waking && !meta.VMLive(main):
		// Unschedulable is the out-of-stock signal: surface it but keep waiting for a seat
		if msg := podUnschedulable(main); msg != "" {
			metrics.SlotReleaseWakeUnschedulableTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
			commonk8s.Eventf(r.Recorder, cs, corev1.EventTypeWarning, "WakeNoCapacity", "main pod %s unschedulable: %s", main.Name, msg)
		}
		return true, ctrl.Result{RequeueAfter: requeueSuspendPoll},
			r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseWaking))

	case waking && meta.VMLive(main):
		vmName := meta.ParseVMSpec(main).VMName
		if err := r.Registry.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
			return true, ctrl.Result{}, fmt.Errorf("wake: drop hibernate snapshot %s: %w", vmName, err)
		}
		// The hint is the wake's in-flight marker; a hint-less stale-status re-entry must not re-score the landing
		if hint != "" {
			logger.Infof(ctx, "wake %s/%s: restored on %s, dropping hibernate snapshot", cs.Namespace, cs.Name, main.Spec.NodeName)
			placement := "pool"
			if hint == main.Spec.NodeName {
				placement = "hint-node"
			}
			metrics.SlotReleaseWakeTotal.WithLabelValues(cs.Namespace, cs.Name, placement).Inc()
			if err := r.clearReleaseRecord(ctx, cs); err != nil {
				return true, ctrl.Result{}, err
			}
		}
		// Auto-derived phase; the requeued pass settles Running/Scaling
		return true, ctrl.Result{RequeueAfter: requeueAfterWrite}, r.patchStatus(ctx, cs, buildStatus(cs, classified, ""))

	case (suspended || suspending) && hint != "" && !meta.IsPodTerminal(main):
		// A stale view of the deleted main or a delete that never ran; only an uncached read tells them apart
		return r.confirmReleasedDelete(ctx, main)

	default:
		// Retained placeholder or kept terminal pod: the normal flow owns these
		return false, ctrl.Result{}, nil
	}
}

// startReleasedWake recreates main with restore intent; handled=false only when no restorable snapshot exists.
func (r *Reconciler) startReleasedWake(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.startReleasedWake")
	vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, 0)
	present, probeErr := snapshot.HasHibernateSnapshot(ctx, r.Registry, vmName)
	if probeErr != nil {
		// Fail closed: falling through would fresh-boot over a real snapshot
		return true, ctrl.Result{}, fmt.Errorf("wake: %w", probeErr)
	}
	if !present {
		// No snapshot means suspended before first boot; the normal flow fresh-boots
		return false, ctrl.Result{}, nil
	}
	discarded, discardErr := r.discardReleasedSnapshotOnImageChange(ctx, cs, vmName)
	if discardErr != nil {
		return true, ctrl.Result{}, fmt.Errorf("wake: %w", discardErr)
	}
	if discarded {
		return false, ctrl.Result{}, nil
	}
	// Persist Waking before the create so a crash between the two resumes here instead of fresh-booting
	if err := r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseWaking)); err != nil {
		return true, ctrl.Result{}, err
	}
	pod, err := buildAgentPod(cs, 0, "", "", r.Scheme)
	if err != nil {
		return true, ctrl.Result{}, fmt.Errorf("wake: build main: %w", err)
	}
	meta.MarkRestoreFromHibernate(pod)
	// Soft-prefer the hibernated-on seat; a spec.nodeName pin already set a required affinity and wins
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

func (r *Reconciler) discardReleasedSnapshotOnImageChange(ctx context.Context, cs *cocoonv1.CocoonSet, vmName string) (bool, error) {
	released := cs.Annotations[annotationHibernatedImage]
	if released == "" || released == cs.Spec.Agent.Image {
		return false, nil
	}
	log.WithFunc("cocoonset.Reconciler.discardReleasedSnapshotOnImageChange").Infof(ctx,
		"%s/%s: image changed from %s to %s since the release, dropping hibernate snapshot %s to boot fresh", cs.Namespace, cs.Name, released, cs.Spec.Agent.Image, vmName)
	if err := r.Registry.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
		return false, fmt.Errorf("drop hibernate snapshot %s: %w", vmName, err)
	}
	return true, r.clearReleaseRecord(ctx, cs)
}

func (r *Reconciler) clearReleaseRecord(ctx context.Context, cs *cocoonv1.CocoonSet) error {
	if err := commonk8s.Patch(ctx, r.Client, cs, func(c *cocoonv1.CocoonSet) {
		delete(c.Annotations, meta.AnnotationHibernatedOnNode)
		delete(c.Annotations, annotationHibernatedImage)
	}); err != nil {
		return fmt.Errorf("clear release record of %s/%s: %w", cs.Namespace, cs.Name, err)
	}
	return nil
}

func hasLivePod(c classifiedPods) bool {
	for _, pod := range c.allByName {
		if !meta.IsPodTerminal(pod) {
			return true
		}
	}
	return false
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
