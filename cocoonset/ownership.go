package cocoonset

import (
	"context"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// listOwnedPods lists pods labeled for this CocoonSet and drops any not
// actually controlled by it, so stale-label pods can't be counted in status
// or affected by suspend/delete. Callers wrap the error with a site-specific noun.
func (r *Reconciler) listOwnedPods(ctx context.Context, cs *cocoonv1.CocoonSet) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(
		ctx, &podList,
		client.InNamespace(cs.Namespace),
		client.MatchingLabels{meta.LabelCocoonSet: cs.Name},
	); err != nil {
		return nil, err
	}
	return filterOwnedPods(podList.Items, cs), nil
}

// filterOwnedPods returns only pods controlled by the given owner,
// filtering out pods with stale labels that belong to a different controller.
func filterOwnedPods(pods []corev1.Pod, owner metav1.Object) []corev1.Pod {
	return slices.DeleteFunc(pods, func(p corev1.Pod) bool {
		return !metav1.IsControlledBy(&p, owner)
	})
}
