package hibernation

import (
	"context"
	"fmt"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

func (r *Reconciler) reconcileWake(ctx context.Context, hib *cocoonv1.CocoonHibernation, pod *corev1.Pod, vmName string) (ctrl.Result, error) {
	logger := log.WithFunc("hibernation.Reconciler.reconcileWake")
	r.announceRetryFromFailed(hib, cocoonv1.HibernationDesireWake)

	if meta.ReadHibernateState(pod) {
		if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, false); err != nil {
			return ctrl.Result{}, fmt.Errorf("clear hibernate annotation: %w", err)
		}
	}

	if vmClonedAndRunning(pod) {
		// Drop the snapshot tag. Non-fatal to the wake, but log at error: a
		// persistent failure (e.g. the registry SA lacking delete permission)
		// silently leaks every hibernate snapshot.
		if err := r.Registry.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
			logger.Errorf(ctx, err, "delete hibernation snapshot %s", vmName)
		}
		if r.firstTransitionAt(hib) {
			observePhaseExit(hib, "ok")
			r.emitNormalf(hib, "WokenActive", "pod %s/%s is running", pod.Namespace, pod.Name)
		}
		return ctrl.Result{}, r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseActive, vmName)
	}

	if phaseDeadlineExceeded(hib, cocoonv1.CocoonHibernationPhaseWaking, wakeTimeout) {
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

// vmClonedAndRunning gates on BOTH container Running and a fresh VMID:
// containerStatuses can momentarily show Running during the pod-recreate →
// wake race, and vk-cocoon rewrites the VMID only after the clone succeeds.
func vmClonedAndRunning(pod *corev1.Pod) bool {
	return meta.IsContainerRunning(pod) && meta.ParseVMRuntime(pod).VMID != ""
}
