package cocoonset

import (
	"context"
	"fmt"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// restorableFromHibernateByCR returns the names of pods whose CocoonHibernation
// is in a phase where a (re)created pod must restore its VM from the :hibernate
// snapshot rather than boot fresh: Hibernated (fully hibernated) or Waking
// (mid-wake; the tag is still present). Phase, not Desire, gates this: Phase
// reaches Hibernated only after the snapshot is confirmed pushed and clears on
// wake, so a leaked snapshot left on an Active agent is correctly excluded.
func (r *Reconciler) restorableFromHibernateByCR(ctx context.Context, namespace string) (map[string]struct{}, error) {
	var hibList cocoonv1.CocoonHibernationList
	if err := r.List(ctx, &hibList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list cocoonhibernations in %s: %w", namespace, err)
	}
	out := make(map[string]struct{})
	for i := range hibList.Items {
		hib := &hibList.Items[i]
		if hib.Spec.PodRef.Name == "" {
			continue
		}
		switch hib.Status.Phase {
		case cocoonv1.CocoonHibernationPhaseHibernated, cocoonv1.CocoonHibernationPhaseWaking:
			out[hib.Spec.PodRef.Name] = struct{}{}
		}
	}
	return out, nil
}

// markRestoreIfHibernated flags a freshly-built pod to restore its VM from the
// :hibernate snapshot when the agent is hibernated (intent) AND the snapshot
// actually exists in the registry. The registry probe is the same lookup vk
// performs at wake, so intent∧present guarantees the restore can proceed; the
// probe also fails closed so a create never silently falls back to a fresh boot
// (which a subsequent re-hibernate would then persist over the real snapshot).
func (r *Reconciler) markRestoreIfHibernated(ctx context.Context, pod *corev1.Pod, intent bool) error {
	if !intent || r.Registry == nil {
		return nil
	}
	vmName := meta.ParseVMSpec(pod).VMName
	if vmName == "" {
		return nil
	}
	present, err := r.Registry.HasManifest(ctx, vmName, meta.HibernateSnapshotTag)
	if err != nil {
		return fmt.Errorf("probe hibernate snapshot %s: %w", vmName, err)
	}
	if present {
		meta.MarkRestoreFromHibernate(pod)
		log.WithFunc("cocoonset.Reconciler.markRestoreIfHibernated").Infof(ctx,
			"pod %s/%s will restore VM %s from :hibernate", pod.Namespace, pod.Name, vmName)
	}
	return nil
}
