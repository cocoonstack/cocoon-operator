package cocoonset

import (
	"context"
	"fmt"
	"sync"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

// restoreIntent returns the namespace's restore-intent set, loaded at most once.
type restoreIntent func() (map[string]struct{}, error)

// newRestoreIntent defers the O(CocoonHibernations) List until a pod is actually built.
func (r *Reconciler) newRestoreIntent(ctx context.Context, namespace string) restoreIntent {
	return sync.OnceValues(func() (map[string]struct{}, error) {
		return r.podsRestorableByCR(ctx, namespace)
	})
}

func (r *Reconciler) hibernationPodNames(ctx context.Context, namespace string, accept func(*cocoonv1.CocoonHibernation) bool) (map[string]struct{}, error) {
	var hibList cocoonv1.CocoonHibernationList
	if err := r.List(ctx, &hibList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list cocoonhibernations in %s: %w", namespace, err)
	}
	out := make(map[string]struct{}, len(hibList.Items))
	for i := range hibList.Items {
		hib := &hibList.Items[i]
		if hib.Spec.PodRef.Name != "" && accept(hib) {
			out[hib.Spec.PodRef.Name] = struct{}{}
		}
	}
	return out, nil
}

// podsRestorableByCR gates on Phase, not Desire; Phase flips only once the push is confirmed.
func (r *Reconciler) podsRestorableByCR(ctx context.Context, namespace string) (map[string]struct{}, error) {
	return r.hibernationPodNames(ctx, namespace, func(h *cocoonv1.CocoonHibernation) bool {
		return h.Status.Phase == cocoonv1.CocoonHibernationPhaseHibernated ||
			h.Status.Phase == cocoonv1.CocoonHibernationPhaseWaking
	})
}

func (r *Reconciler) markRestoreFromIntent(ctx context.Context, pod *corev1.Pod, intent restoreIntent) error {
	restorable, err := intent()
	if err != nil {
		return err
	}
	_, want := restorable[pod.Name]
	return r.markRestoreIfHibernated(ctx, pod, want)
}

// markRestoreIfHibernated fails closed; a fresh boot on probe error would let a re-hibernate overwrite the snapshot.
func (r *Reconciler) markRestoreIfHibernated(ctx context.Context, pod *corev1.Pod, intent bool) error {
	logger := log.WithFunc("cocoonset.Reconciler.markRestoreIfHibernated")
	if !intent || r.Registry == nil {
		return nil
	}
	vmName := meta.ParseVMSpec(pod).VMName
	present, err := snapshot.HasHibernateSnapshot(ctx, r.Registry, vmName)
	if err != nil {
		return err
	}
	if present {
		meta.MarkRestoreFromHibernate(pod)
		logger.Infof(ctx, "pod %s/%s will restore VM %s from :hibernate", pod.Namespace, pod.Name, vmName)
	}
	return nil
}
