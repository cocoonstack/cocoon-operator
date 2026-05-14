package hibernation

import (
	"context"
	"fmt"
	"time"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

// reconcileWake clears the hibernate annotation and waits for the container to run.
func (r *Reconciler) reconcileWake(ctx context.Context, hib *cocoonv1.CocoonHibernation, pod *corev1.Pod, vmName string) (ctrl.Result, error) {
	logger := log.WithFunc("hibernation.Reconciler.reconcileWake")
	r.announceRetryFromFailed(hib, cocoonv1.HibernationDesireWake)

	if vmClonedAndRunning(pod) {
		// Drop snapshot tag (non-fatal; stale tag gets overwritten on next hibernate).
		if err := r.Epoch.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
			logger.Warnf(ctx, "delete hibernation snapshot %s: %v", vmName, err)
		}
		if r.firstTransitionAt(hib) {
			observePhaseExit(hib, "ok")
			r.emitNormalf(hib, "WokenActive", "pod %s/%s is running", pod.Namespace, pod.Name)
		}
		return ctrl.Result{}, r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseActive, vmName)
	}

	if meta.ReadHibernateState(pod) {
		if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, false); err != nil {
			return ctrl.Result{}, fmt.Errorf("clear hibernate annotation: %w", err)
		}
	}

	if wakeDeadlineExceeded(hib) {
		if r.firstTransitionAt(hib) {
			observePhaseExit(hib, "timeout")
			r.emitWarningf(hib, "WakeTimedOut", "vk-cocoon did not report the container running within %s", wakeTimeout)
		}
		return ctrl.Result{}, r.markFailed(ctx, hib,
			fmt.Sprintf("wake timed out after %s; vk-cocoon never reported the container running", wakeTimeout))
	}

	if err := r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseWaking, vmName); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// vmClonedAndRunning reports whether vk-cocoon has finished cloning from the
// hibernation snapshot and the container is live on the new VM. Gated on BOTH
// container Running AND a freshly applied VMID: during hibernate, vk-cocoon
// clears the VMID annotation; on wake it writes a new VMID only after the
// snapshot clone succeeds. Checking IsContainerRunning alone is unreliable
// because pod.status.containerStatuses can momentarily show Running during
// the pod-recreate → wake race, causing the snapshot tag to be dropped before
// vk-cocoon has pulled it.
func vmClonedAndRunning(pod *corev1.Pod) bool {
	return meta.IsContainerRunning(pod) && meta.ParseVMRuntime(pod).VMID != ""
}

// wakeDeadlineExceeded checks whether the Waking phase has exceeded wakeTimeout.
func wakeDeadlineExceeded(hib *cocoonv1.CocoonHibernation) bool {
	if hib.Status.Phase != cocoonv1.CocoonHibernationPhaseWaking {
		return false
	}
	ready := apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady)
	if ready == nil || ready.LastTransitionTime.IsZero() {
		return false
	}
	return time.Since(ready.LastTransitionTime.Time) > wakeTimeout
}
