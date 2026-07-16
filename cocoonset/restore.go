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

// newRestoreIntent defers the List until a pod actually has to be built: it is
// O(CocoonHibernations in the namespace) and the steady path must not pay it.
func (r *Reconciler) newRestoreIntent(ctx context.Context, namespace string) restoreIntent {
	return sync.OnceValues(func() (map[string]struct{}, error) {
		return r.podsRestorableByCR(ctx, namespace)
	})
}

// hibernationPodNames lists the namespace's CocoonHibernations and returns the
// set of PodRef names whose CR satisfies accept.
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

// podsRestorableByCR returns pod names whose CocoonHibernation phase (Hibernated,
// or Waking with the tag still present) requires a (re)created pod to restore
// rather than boot fresh. Phase gates this, not Desire: Phase only reaches
// Hibernated once the push is confirmed, so a leaked tag on an Active agent is excluded.
func (r *Reconciler) podsRestorableByCR(ctx context.Context, namespace string) (map[string]struct{}, error) {
	return r.hibernationPodNames(ctx, namespace, func(h *cocoonv1.CocoonHibernation) bool {
		return h.Status.Phase == cocoonv1.CocoonHibernationPhaseHibernated ||
			h.Status.Phase == cocoonv1.CocoonHibernationPhaseWaking
	})
}

// markRestoreIfHibernated flags a freshly-built pod to restore from :hibernate
// when intent holds and the snapshot exists in the registry. Fails closed: a
// probe error must not fall back to a fresh boot that a later re-hibernate
// would persist over the real snapshot.
func (r *Reconciler) markRestoreIfHibernated(ctx context.Context, pod *corev1.Pod, intent bool) error {
	logger := log.WithFunc("cocoonset.Reconciler.markRestoreIfHibernated")
	if !intent || r.Registry == nil {
		return nil
	}
	vmName := meta.ParseVMSpec(pod).VMName
	if vmName == "" {
		return nil
	}
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
