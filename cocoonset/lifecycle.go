package cocoonset

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
)

// syncCocoonSetGeneration writes cs.Generation to each owned pod so
// vk-cocoon can echo it back as lifecycle-observed-generation, giving
// clients a counter-based completion signal immune to wallclock skew.
func (r *Reconciler) syncCocoonSetGeneration(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) error {
	return classified.forEachSorted(ctx, func(pod *corev1.Pod) error {
		if err := commonk8s.PatchCocoonSetGeneration(ctx, r.Client, pod, cs.Generation); err != nil {
			return fmt.Errorf("patch cocoonset generation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		return nil
	})
}
