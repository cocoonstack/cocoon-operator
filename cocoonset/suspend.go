package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

// reconcileSuspend ensures the main agent exists, applies the hibernate
// annotation to every owned pod, then polls the registry to observe when all
// managed VMs have been pushed to snapshot. Stays in Suspending with a
// periodic requeue until every required snapshot lands.
func (r *Reconciler) reconcileSuspend(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.reconcileSuspend")
	if classified.main == nil {
		mainPod, err := buildAgentPod(cs, 0, "", "", r.Scheme)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("build main agent before suspend: %w", err)
		}
		// reconcileSuspend only runs under Spec.Suspend, so restore intent is unconditional.
		if err := r.markRestoreIfHibernated(ctx, mainPod, true); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, mainPod); err != nil {
			return ctrl.Result{}, fmt.Errorf("create main agent before suspend: %w", err)
		}
		logger.Infof(ctx, "created main agent %s/%s ahead of suspend", mainPod.Namespace, mainPod.Name)
		return ctrl.Result{Requeue: true}, nil
	}
	if err := r.applySuspend(ctx, classified); err != nil {
		return ctrl.Result{}, err
	}
	allHibernated, err := r.allOwnedPodsHibernated(ctx, classified)
	if err != nil {
		return ctrl.Result{}, err
	}
	phase := cocoonv1.CocoonSetPhaseSuspending
	result := ctrl.Result{RequeueAfter: requeueSuspendPoll}
	if allHibernated {
		phase = cocoonv1.CocoonSetPhaseSuspended
		result = ctrl.Result{}
	}
	return result, r.patchStatus(ctx, cs, buildStatus(cs, classified, phase))
}

// allOwnedPodsHibernated reports whether every managed owned pod has a
// hibernate snapshot published to the registry. Unmanaged pods (e.g. static
// toolboxes) are skipped since they have no VM lifecycle to observe.
// Returns (false, nil) whenever the expected state is not yet observed so
// the caller requeues rather than treats it as an error.
func (r *Reconciler) allOwnedPodsHibernated(ctx context.Context, classified classifiedPods) (bool, error) {
	if r.Registry == nil {
		// No registry configured; such deployments have no snapshot to
		// observe, so treat the annotation write as authoritative.
		return true, nil
	}
	for _, name := range slices.Sorted(maps.Keys(classified.allByName)) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		pod := classified.allByName[name]
		spec := meta.ParseVMSpec(pod)
		if !spec.Managed {
			continue
		}
		if spec.VMName == "" {
			return false, nil
		}
		present, err := snapshot.HasHibernateSnapshot(ctx, r.Registry, spec.VMName)
		if err != nil {
			return false, err
		}
		if !present {
			return false, nil
		}
	}
	return true, nil
}

// applySuspend writes HibernateState(true) onto every owned pod.
func (r *Reconciler) applySuspend(ctx context.Context, classified classifiedPods) error {
	return classified.forEachSorted(ctx, func(pod *corev1.Pod) error {
		if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, true); err != nil {
			return fmt.Errorf("patch hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		return nil
	})
}

// applyUnsuspend clears HibernateState from owned pods, skipping pods that are
// targets of an active CocoonHibernation CR to avoid racing the hibernation
// reconciler. The CR list loads lazily so the steady path never pays it.
func (r *Reconciler) applyUnsuspend(ctx context.Context, namespace string, classified classifiedPods) error {
	var hibernatedByCR map[string]struct{}
	return classified.forEachSorted(ctx, func(pod *corev1.Pod) error {
		if !meta.ReadHibernateState(pod) {
			return nil
		}
		if hibernatedByCR == nil {
			var err error
			if hibernatedByCR, err = r.podsHibernatedByCR(ctx, namespace); err != nil {
				return err
			}
		}
		if _, ownedByCR := hibernatedByCR[pod.Name]; ownedByCR {
			return nil
		}
		if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, false); err != nil {
			return fmt.Errorf("clear hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		return nil
	})
}

// podsHibernatedByCR returns pod names targeted by a desire=Hibernate CR.
func (r *Reconciler) podsHibernatedByCR(ctx context.Context, namespace string) (map[string]struct{}, error) {
	return r.hibernationPodNames(ctx, namespace, func(h *cocoonv1.CocoonHibernation) bool {
		return h.Spec.Desire == cocoonv1.HibernationDesireHibernate
	})
}
