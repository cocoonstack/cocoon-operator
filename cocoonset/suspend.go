package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/podpatch"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

// reconcileSuspend polls the registry and stays Suspending until every managed VM's snapshot lands.
func (r *Reconciler) reconcileSuspend(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (ctrl.Result, error) {
	if classified.main == nil {
		// reconcileSuspend only runs under Spec.Suspend, so restore intent is unconditional.
		return r.createMainAgent(ctx, cs, func() (map[string]struct{}, error) {
			return map[string]struct{}{agentPodName(cs.Name, 0): {}}, nil
		})
	}
	if err := r.applySuspend(ctx, classified); err != nil {
		return ctrl.Result{}, err
	}
	allHibernated, err := r.allOwnedPodsHibernated(ctx, cs, classified)
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

// allOwnedPodsHibernated returns (false, nil), not an error, while the expected state is not yet observed.
func (r *Reconciler) allOwnedPodsHibernated(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, error) {
	for _, name := range slices.Sorted(maps.Keys(classified.allByName)) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		pod := classified.allByName[name]
		spec := meta.ParseVMSpec(pod)
		if !spec.Managed {
			continue
		}
		// a kubelet-terminal pod has no VM to snapshot; waiting on it would park the set in Suspending forever
		if meta.IsPodTerminal(pod) {
			continue
		}
		if spec.VMName == "" {
			return false, nil
		}
		// vk flips hibernated with observed-generation only after this round's push; a stale tag or lagging informer cannot pass
		if st := meta.ReadLifecycleStatus(pod); st.State != meta.LifecycleStateHibernated ||
			st.ObservedGeneration < cs.Generation {
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

func (r *Reconciler) applySuspend(ctx context.Context, classified classifiedPods) error {
	return classified.forEachSorted(ctx, func(pod *corev1.Pod) error {
		if err := podpatch.HibernateState(ctx, r.Client, pod, true); err != nil {
			return fmt.Errorf("patch hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		return nil
	})
}

// applyUnsuspend skips pods targeted by an active CocoonHibernation CR to avoid racing that reconciler.
func (r *Reconciler) applyUnsuspend(ctx context.Context, namespace string, classified classifiedPods) error {
	var hibernated []*corev1.Pod
	for _, name := range slices.Sorted(maps.Keys(classified.allByName)) {
		if pod := classified.allByName[name]; meta.ReadHibernateState(pod) {
			hibernated = append(hibernated, pod)
		}
	}
	if len(hibernated) == 0 {
		return nil
	}

	hibernatedByCR, err := r.podsHibernatedByCR(ctx, namespace)
	if err != nil {
		return err
	}
	for _, pod := range hibernated {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if _, ownedByCR := hibernatedByCR[pod.Name]; ownedByCR {
			continue
		}
		if err := podpatch.HibernateState(ctx, r.Client, pod, false); err != nil {
			return fmt.Errorf("clear hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// podsHibernatedByCR returns pod names whose CR desires Hibernate or is already Hibernating.
func (r *Reconciler) podsHibernatedByCR(ctx context.Context, namespace string) (map[string]struct{}, error) {
	return r.hibernationPodNames(ctx, namespace, func(h *cocoonv1.CocoonHibernation) bool {
		return h.Spec.Desire == cocoonv1.HibernationDesireHibernate || h.Status.Phase == cocoonv1.CocoonHibernationPhaseHibernating
	})
}
