package cocoonset

import (
	"maps"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/cocoonstack/cocoon-common/meta"
)

// podRelevantChange drops pure status churn that would otherwise storm reconciles.
type podRelevantChange struct{}

func (podRelevantChange) Create(_ event.CreateEvent) bool   { return true }
func (podRelevantChange) Delete(_ event.DeleteEvent) bool   { return true }
func (podRelevantChange) Generic(_ event.GenericEvent) bool { return false }

func (podRelevantChange) Update(e event.UpdateEvent) bool {
	oldPod, ok1 := e.ObjectOld.(*corev1.Pod)
	newPod, ok2 := e.ObjectNew.(*corev1.Pod)
	if !ok1 || !ok2 {
		return true
	}
	return (oldPod.DeletionTimestamp.IsZero() && !newPod.DeletionTimestamp.IsZero()) ||
		oldPod.Status.Phase != newPod.Status.Phase ||
		meta.IsPodReady(oldPod) != meta.IsPodReady(newPod) ||
		!maps.Equal(oldPod.Labels, newPod.Labels) ||
		!maps.Equal(oldPod.Annotations, newPod.Annotations)
}
