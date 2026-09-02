package hibernation

import (
	"context"
	"fmt"

	"github.com/cocoonstack/cocoon-operator/podpatch"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

func (r *Reconciler) reconcileHibernate(ctx context.Context, hib *cocoonv1.CocoonHibernation, pod *corev1.Pod, vmName string) (ctrl.Result, error) {
	r.announceRetryFromFailed(hib, cocoonv1.HibernationDesireHibernate)

	if err := podpatch.HibernateState(ctx, r.Client, pod, true); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch hibernate annotation: %w", err)
	}

	// gate on vk's completion signal for this generation before the probe; a stale tag from a prior cycle would satisfy it immediately
	if st := meta.ReadLifecycleStatus(pod); st.State == meta.LifecycleStateHibernated &&
		st.ObservedGeneration >= meta.ReadCocoonSetGeneration(pod) {
		present, err := snapshot.HasHibernateSnapshot(ctx, r.Registry, vmName)
		// a persistently failing probe must still hit the deadline below, or the phase starves in Hibernating
		if err != nil && !phaseDeadlineExceeded(hib, cocoonv1.CocoonHibernationPhaseHibernating, hibernateTimeout) {
			return ctrl.Result{}, err
		}
		if err == nil && present {
			r.announcePhaseExitf(hib, "ok", corev1.EventTypeNormal, "Hibernated", "snapshot %s pushed to the registry", vmName)
			return ctrl.Result{}, r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseHibernated, vmName)
		}
	}
	if phaseDeadlineExceeded(hib, cocoonv1.CocoonHibernationPhaseHibernating, hibernateTimeout) {
		r.announcePhaseExitf(hib, "timeout", corev1.EventTypeWarning, "HibernateTimedOut",
			"snapshot %s not confirmed in the registry within %s", vmName, hibernateTimeout)
		return ctrl.Result{}, r.markFailed(ctx, hib,
			fmt.Sprintf("hibernate not confirmed within %s", hibernateTimeout))
	}
	if updateErr := r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseHibernating, vmName); updateErr != nil {
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}
