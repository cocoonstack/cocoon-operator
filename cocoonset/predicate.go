package cocoonset

import (
	"maps"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/cocoonstack/cocoon-common/meta"
)

// podRelevantChange filters pod events to those that affect CocoonSet
// reconciliation: creation, deletion, and readiness transitions.
// Ignores pure status churn (VK notify loops, condition timestamp updates).
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
	// Deletion timestamp set → pod being deleted.
	if oldPod.DeletionTimestamp.IsZero() && !newPod.DeletionTimestamp.IsZero() {
		return true
	}
	// Phase changed.
	if oldPod.Status.Phase != newPod.Status.Phase {
		return true
	}
	// Readiness changed.
	if meta.IsPodReady(oldPod) != meta.IsPodReady(newPod) {
		return true
	}
	// Labels or annotations changed (spec drift, runtime annotations).
	if !maps.Equal(oldPod.Labels, newPod.Labels) {
		return true
	}
	if !maps.Equal(oldPod.Annotations, newPod.Annotations) {
		return true
	}
	return false
}
