package cocoonset

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// filterOwnedPods returns only pods controlled by the given owner,
// filtering out pods with stale labels that belong to a different controller.
func filterOwnedPods(pods []corev1.Pod, owner metav1.Object) []corev1.Pod {
	return slices.DeleteFunc(pods, func(p corev1.Pod) bool {
		return !metav1.IsControlledBy(&p, owner)
	})
}
