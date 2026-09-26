package cocoonset

import (
	"maps"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

var setRelevantChange = predicate.Or(predicate.GenerationChangedPredicate{}, predicate.Funcs{UpdateFunc: setStatusChanged})

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

func setStatusChanged(e event.UpdateEvent) bool {
	oldSet, ok1 := e.ObjectOld.(*cocoonv1.CocoonSet)
	newSet, ok2 := e.ObjectNew.(*cocoonv1.CocoonSet)
	return !ok1 || !ok2 || !equality.Semantic.DeepEqual(oldSet.Status, newSet.Status)
}
