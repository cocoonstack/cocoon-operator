// Package hibernation drives hibernate/wake transitions for CocoonHibernation CRs.
package hibernation

import (
	"context"
	"fmt"
	"time"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/epoch"
)

const (
	requeueInterval = 5 * time.Second
	// wakeTimeout bounds how long Waking can last before marking Failed.
	wakeTimeout = 5 * time.Minute

	conditionReasonPending = "Pending"
	conditionReasonDone    = "Done"
	conditionReasonFailed  = "Failed"
)

type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Epoch  epoch.SnapshotRegistry
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cocoonv1.CocoonHibernation{}).
		Complete(r)
}

// Reconcile drives a single hibernate or wake transition. Failed phases are recoverable.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.WithFunc("hibernation.Reconciler.Reconcile")

	var hib cocoonv1.CocoonHibernation
	if err := r.Get(ctx, req.NamespacedName, &hib); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get hibernation %s: %w", req.NamespacedName, err)
	}

	if hib.Spec.PodRef.Name == "" {
		return ctrl.Result{}, r.markFailed(ctx, &hib, "spec.podRef.name is required")
	}
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: hib.Namespace, Name: hib.Spec.PodRef.Name}, &pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.markFailed(ctx, &hib, fmt.Sprintf("pod %s/%s not found", hib.Namespace, hib.Spec.PodRef.Name))
		}
		return ctrl.Result{}, fmt.Errorf("get target pod: %w", err)
	}

	vmName := meta.ParseVMSpec(&pod).VMName
	if vmName == "" {
		return ctrl.Result{}, r.markFailed(ctx, &hib, fmt.Sprintf("pod %s/%s has no %s annotation", pod.Namespace, pod.Name, meta.AnnotationVMName))
	}

	logger.Debugf(ctx, "reconcile hibernation %s/%s desire=%s vm=%s", hib.Namespace, hib.Name, hib.Spec.Desire, vmName)

	switch hib.Spec.Desire {
	case cocoonv1.HibernationDesireHibernate:
		return r.reconcileHibernate(ctx, &hib, &pod, vmName)
	case cocoonv1.HibernationDesireWake:
		return r.reconcileWake(ctx, &hib, &pod, vmName)
	default:
		return ctrl.Result{}, r.markFailed(ctx, &hib, fmt.Sprintf("unknown desire %q", hib.Spec.Desire))
	}
}

// reconcileHibernate sets hibernate annotation and polls epoch for the snapshot tag.
func (r *Reconciler) reconcileHibernate(ctx context.Context, hib *cocoonv1.CocoonHibernation, pod *corev1.Pod, vmName string) (ctrl.Result, error) {
	if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, true); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch hibernate annotation: %w", err)
	}

	present, err := r.Epoch.HasManifest(ctx, vmName, meta.HibernateSnapshotTag)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("probe hibernate snapshot %s: %w", vmName, err)
	}
	if present {
		return ctrl.Result{}, r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseHibernated, vmName)
	}
	// Snapshot not yet pushed — keep polling.
	if updateErr := r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseHibernating, vmName); updateErr != nil {
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// reconcileWake clears the hibernate annotation and waits for the container to run.
func (r *Reconciler) reconcileWake(ctx context.Context, hib *cocoonv1.CocoonHibernation, pod *corev1.Pod, vmName string) (ctrl.Result, error) {
	logger := log.WithFunc("hibernation.Reconciler.reconcileWake")
	if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, false); err != nil {
		return ctrl.Result{}, fmt.Errorf("clear hibernate annotation: %w", err)
	}

	if meta.IsContainerRunning(pod) {
		// Drop snapshot tag (non-fatal; stale tag gets overwritten on next hibernate).
		if err := r.Epoch.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
			logger.Warnf(ctx, "delete hibernation snapshot %s: %v", vmName, err)
		}
		return ctrl.Result{}, r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseActive, vmName)
	}

	if wakeDeadlineExceeded(hib) {
		return ctrl.Result{}, r.markFailed(ctx, hib,
			fmt.Sprintf("wake timed out after %s; vk-cocoon never reported the container running", wakeTimeout))
	}

	if err := r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseWaking, vmName); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// wakeDeadlineExceeded checks whether the Waking phase has exceeded wakeTimeout.
func wakeDeadlineExceeded(hib *cocoonv1.CocoonHibernation) bool {
	if hib.Status.Phase != cocoonv1.CocoonHibernationPhaseWaking {
		return false
	}
	ready := apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady)
	if ready == nil || ready.LastTransitionTime.IsZero() {
		return false
	}
	return time.Since(ready.LastTransitionTime.Time) > wakeTimeout
}

// setPhase patches status, preserving timestamps on no-op updates.
// On Failed->Waking re-entry, it refreshes LastTransitionTime so the wake deadline
// does not inherit the stale timestamp from the previous failure.
func (r *Reconciler) setPhase(ctx context.Context, hib *cocoonv1.CocoonHibernation, phase cocoonv1.CocoonHibernationPhase, vmName string) error {
	if hib.Status.Phase == phase && hib.Status.VMName == vmName {
		return nil
	}
	refreshWakeDeadline := phase == cocoonv1.CocoonHibernationPhaseWaking &&
		hib.Status.Phase != cocoonv1.CocoonHibernationPhaseWaking
	if err := commonk8s.PatchStatus(ctx, r.Client, hib, func(h *cocoonv1.CocoonHibernation) {
		h.Status.ObservedGeneration = h.Generation
		h.Status.Phase = phase
		h.Status.VMName = vmName
		apimeta.SetStatusCondition(&h.Status.Conditions, readyCondition(phase, h.Generation))
		if refreshWakeDeadline {
			if ready := apimeta.FindStatusCondition(h.Status.Conditions, commonk8s.ConditionTypeReady); ready != nil {
				ready.LastTransitionTime = metav1.Now()
			}
		}
	}); err != nil {
		return fmt.Errorf("patch hibernation status: %w", err)
	}
	return nil
}

// markFailed sets the Failed phase. A subsequent reconcile can recover by overwriting it.
func (r *Reconciler) markFailed(ctx context.Context, hib *cocoonv1.CocoonHibernation, msg string) error {
	if err := commonk8s.PatchStatus(ctx, r.Client, hib, func(h *cocoonv1.CocoonHibernation) {
		h.Status.ObservedGeneration = h.Generation
		h.Status.Phase = cocoonv1.CocoonHibernationPhaseFailed
		apimeta.SetStatusCondition(&h.Status.Conditions, commonk8s.NewReadyCondition(
			h.Generation, metav1.ConditionFalse, conditionReasonFailed, msg,
		))
	}); err != nil {
		return fmt.Errorf("patch failed status: %w", err)
	}
	return nil
}

// readyCondition maps a phase to a Ready condition with zero timestamp for merge safety.
func readyCondition(phase cocoonv1.CocoonHibernationPhase, generation int64) metav1.Condition {
	switch phase {
	case cocoonv1.CocoonHibernationPhaseHibernated, cocoonv1.CocoonHibernationPhaseActive:
		return commonk8s.NewReadyCondition(generation, metav1.ConditionTrue, conditionReasonDone, string(phase))
	case cocoonv1.CocoonHibernationPhaseFailed:
		return commonk8s.NewReadyCondition(generation, metav1.ConditionFalse, conditionReasonFailed, string(phase))
	default:
		return commonk8s.NewReadyCondition(generation, metav1.ConditionFalse, conditionReasonPending, string(phase))
	}
}
