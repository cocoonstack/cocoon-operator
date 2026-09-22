package hibernation

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/podpatch"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

func (r *Reconciler) reconcileWake(ctx context.Context, hib *cocoonv1.CocoonHibernation, pod *corev1.Pod, vmName string) (ctrl.Result, error) {
	settled := hib.Status.Phase == cocoonv1.CocoonHibernationPhaseActive && hib.Status.ObservedGeneration == hib.Generation
	if settled && bool(meta.ReadHibernateState(pod)) {
		return ctrl.Result{}, nil
	}
	if !settled {
		r.announceRetryFromFailed(hib, cocoonv1.HibernationDesireWake)
		if meta.ReadHibernateState(pod) {
			if err := podpatch.HibernateState(ctx, r.Client, pod, false); err != nil {
				return ctrl.Result{}, fmt.Errorf("clear hibernate annotation: %w", err)
			}
		}
	}

	if meta.VMLive(pod) {
		if !settled {
			r.announcePhaseExitf(hib, "ok", corev1.EventTypeNormal, "WokenActive", "pod %s/%s is running", pod.Namespace, pod.Name)
			if err := r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseActive, vmName); err != nil {
				return ctrl.Result{}, err
			}
		}
		// the live VM owns the state now; a surviving tag would roll a recreated pod back to it
		if err := snapshot.DeleteManifestIfPresent(ctx, r.Registry, vmName, meta.HibernateSnapshotTag); err != nil {
			return ctrl.Result{}, fmt.Errorf("delete hibernation snapshot %s: %w", vmName, err)
		}
		return ctrl.Result{}, nil
	}
	if settled {
		return ctrl.Result{}, nil
	}

	if phaseDeadlineExceeded(hib, cocoonv1.CocoonHibernationPhaseWaking, wakeTimeout) {
		r.announcePhaseExitf(hib, "timeout", corev1.EventTypeWarning, "WakeTimedOut",
			"vk-cocoon did not report the container running within %s", wakeTimeout)
		return ctrl.Result{}, r.markFailed(ctx, hib,
			fmt.Sprintf("wake timed out after %s; vk-cocoon never reported the container running", wakeTimeout))
	}

	if err := r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseWaking, vmName); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}
