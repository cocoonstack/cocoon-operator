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
)

// restoreIntent memoizes the namespace's restore-intent set for one reconcile.
// The List behind it is O(CocoonHibernations in the namespace), so the steady
// path (every desired pod already present) must never pay it, and the agent and
// toolbox passes must not pay it twice.
type restoreIntent struct {
	load  func(context.Context) (map[string]struct{}, error)
	once  sync.Once
	names map[string]struct{}
	err   error
}

// resolve loads the set on first call; safe under the sub-agent create fan-out.
func (ri *restoreIntent) resolve(ctx context.Context) (map[string]struct{}, error) {
	ri.once.Do(func() { ri.names, ri.err = ri.load(ctx) })
	return ri.names, ri.err
}

// newRestoreIntent defers the List until a pod actually has to be built.
func (r *Reconciler) newRestoreIntent(namespace string) *restoreIntent {
	return &restoreIntent{load: func(ctx context.Context) (map[string]struct{}, error) {
		return r.podsRestorableByCR(ctx, namespace)
	}}
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

// podsRestorableByCR returns pod names whose CocoonHibernation is in a phase where
// a (re)created pod must restore its VM from the :hibernate snapshot rather than
// boot fresh: Hibernated (fully hibernated) or Waking (mid-wake; the tag is still
// present). Phase, not Desire, gates this: Phase reaches Hibernated only after the
// snapshot is confirmed pushed and clears on wake, so a leaked snapshot left on an
// Active agent is correctly excluded.
func (r *Reconciler) podsRestorableByCR(ctx context.Context, namespace string) (map[string]struct{}, error) {
	return r.hibernationPodNames(ctx, namespace, func(h *cocoonv1.CocoonHibernation) bool {
		return h.Status.Phase == cocoonv1.CocoonHibernationPhaseHibernated ||
			h.Status.Phase == cocoonv1.CocoonHibernationPhaseWaking
	})
}

// hasHibernateSnapshot reports whether vmName has a :hibernate snapshot in the
// registry — the same lookup vk-cocoon performs at wake.
func (r *Reconciler) hasHibernateSnapshot(ctx context.Context, vmName string) (bool, error) {
	present, err := r.Registry.HasManifest(ctx, vmName, meta.HibernateSnapshotTag)
	if err != nil {
		return false, fmt.Errorf("probe hibernate snapshot %s: %w", vmName, err)
	}
	return present, nil
}

// markRestoreIfHibernated flags a freshly-built pod to restore its VM from the
// :hibernate snapshot when the agent is hibernated (intent) and the snapshot
// actually exists in the registry. The probe is the same lookup vk runs at wake,
// so intent and a present snapshot together guarantee the restore can proceed; it
// also fails closed so a create never silently falls back to a fresh boot (which a
// subsequent re-hibernate would then persist over the real snapshot).
func (r *Reconciler) markRestoreIfHibernated(ctx context.Context, pod *corev1.Pod, intent bool) error {
	logger := log.WithFunc("cocoonset.Reconciler.markRestoreIfHibernated")
	if !intent || r.Registry == nil {
		return nil
	}
	vmName := meta.ParseVMSpec(pod).VMName
	if vmName == "" {
		return nil
	}
	present, err := r.hasHibernateSnapshot(ctx, vmName)
	if err != nil {
		return err
	}
	if present {
		meta.MarkRestoreFromHibernate(pod)
		logger.Infof(ctx, "pod %s/%s will restore VM %s from :hibernate", pod.Namespace, pod.Name, vmName)
	}
	return nil
}
