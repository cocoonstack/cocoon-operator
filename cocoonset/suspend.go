package cocoonset

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

// allOwnedPodsHibernated reports whether every managed owned pod has a
// hibernate snapshot published to epoch. Unmanaged pods (e.g. static
// toolboxes) are skipped since they have no VM lifecycle to observe.
// Returns (false, nil) whenever the expected state is not yet observed so
// the caller requeues rather than treats it as an error.
func (r *Reconciler) allOwnedPodsHibernated(ctx context.Context, classified classifiedPods) (bool, error) {
	if r.Epoch == nil {
		// No registry configured — fall back to "trust the annotation write"
		// so existing deployments without epoch still reach Suspended.
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
		present, err := r.Epoch.HasManifest(ctx, spec.VMName, meta.HibernateSnapshotTag)
		if err != nil {
			return false, fmt.Errorf("probe hibernate snapshot %s: %w", spec.VMName, err)
		}
		if !present {
			return false, nil
		}
	}
	return true, nil
}

// applySuspend writes HibernateState(true) onto every owned pod.
func (r *Reconciler) applySuspend(ctx context.Context, classified classifiedPods) error {
	for _, name := range slices.Sorted(maps.Keys(classified.allByName)) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		pod := classified.allByName[name]
		if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, true); err != nil {
			return fmt.Errorf("patch hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// applyUnsuspend clears HibernateState from owned pods, skipping pods that are
// targets of an active CocoonHibernation CR to avoid racing the hibernation reconciler.
func (r *Reconciler) applyUnsuspend(ctx context.Context, namespace string, classified classifiedPods) error {
	var hibernated []*corev1.Pod
	for _, pod := range classified.allByName {
		if meta.ReadHibernateState(pod) {
			hibernated = append(hibernated, pod)
		}
	}
	if len(hibernated) == 0 {
		return nil
	}
	slices.SortFunc(hibernated, func(a, b *corev1.Pod) int {
		return cmp.Compare(a.Name, b.Name)
	})

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
		if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, false); err != nil {
			return fmt.Errorf("clear hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// podsHibernatedByCR returns pod names targeted by a desire=Hibernate CR.
func (r *Reconciler) podsHibernatedByCR(ctx context.Context, namespace string) (map[string]struct{}, error) {
	var hibList cocoonv1.CocoonHibernationList
	if err := r.List(ctx, &hibList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list cocoonhibernations in %s: %w", namespace, err)
	}
	out := make(map[string]struct{}, len(hibList.Items))
	for i := range hibList.Items {
		hib := &hibList.Items[i]
		if hib.Spec.Desire != cocoonv1.HibernationDesireHibernate {
			continue
		}
		if hib.Spec.PodRef.Name == "" {
			continue
		}
		out[hib.Spec.PodRef.Name] = struct{}{}
	}
	return out, nil
}
