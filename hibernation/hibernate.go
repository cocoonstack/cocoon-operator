package hibernation

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

func (r *Reconciler) reconcileHibernate(ctx context.Context, hib *cocoonv1.CocoonHibernation, pod *corev1.Pod, vmName string) (ctrl.Result, error) {
	r.announceRetryFromFailed(hib, cocoonv1.HibernationDesireHibernate)

	if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, true); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch hibernate annotation: %w", err)
	}

	present, err := r.Registry.HasManifest(ctx, vmName, meta.HibernateSnapshotTag)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("probe hibernate snapshot %s: %w", vmName, err)
	}
	if present {
		if r.firstTransitionAt(hib) {
			observePhaseExit(hib, "ok")
			r.emitNormalf(hib, "Hibernated", "snapshot %s pushed to the registry", vmName)
		}
		return ctrl.Result{}, r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseHibernated, vmName)
	}
	if phaseDeadlineExceeded(hib, cocoonv1.CocoonHibernationPhaseHibernating, hibernateTimeout) {
		if r.firstTransitionAt(hib) {
			observePhaseExit(hib, "timeout")
			r.emitWarningf(hib, "HibernateTimedOut", "vk-cocoon did not push snapshot %s within %s", vmName, hibernateTimeout)
		}
		return ctrl.Result{}, r.markFailed(ctx, hib,
			fmt.Sprintf("hibernate timed out after %s; vk-cocoon never pushed the snapshot", hibernateTimeout))
	}
	// Snapshot not yet pushed — keep polling.
	if updateErr := r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseHibernating, vmName); updateErr != nil {
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}
