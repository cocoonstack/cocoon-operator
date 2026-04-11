// Package hibernation hosts the CocoonHibernation reconciler. It
// drives a single hibernate / wake transition per CocoonHibernation
// CR by toggling the hibernate annotation on the target pod and
// polling epoch for the snapshot tag.
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
	// requeueInterval is how long the reconciler waits between
	// probes to epoch / pod status while a hibernation or wake
	// transition is in flight.
	requeueInterval = 5 * time.Second

	// wakeTimeout bounds how long a CocoonHibernation can stay at
	// Waking before the reconciler gives up and marks it Failed.
	// Without this budget a silently broken wake (e.g. vk-cocoon
	// unable to pull the snapshot, or cocoon clone looping) would
	// leave the CR stuck at Waking forever — IsContainerRunning is
	// a local probe with no error channel, so only a timeout can
	// surface the failure to the user.
	wakeTimeout = 5 * time.Minute

	conditionReasonPending = "Pending"
	conditionReasonDone    = "Done"
	conditionReasonFailed  = "Failed"
)

// Reconciler reconciles a CocoonHibernation object.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Epoch  epoch.SnapshotRegistry
}

// SetupWithManager registers the reconciler against the supplied
// controller-runtime manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cocoonv1.CocoonHibernation{}).
		Complete(r)
}

// Reconcile drives a single hibernate or wake transition for one
// pod. Each invocation either completes the transition (no requeue)
// or schedules another probe a few seconds later. A previous Failed
// phase is recoverable: a successful path through the switch below
// will overwrite the failure with the new in-flight state.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.WithFunc("hibernation.Reconciler.Reconcile")

	var hib cocoonv1.CocoonHibernation
	if err := r.Get(ctx, req.NamespacedName, &hib); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get hibernation %s: %w", req.NamespacedName, err)
	}

	// Resolve the target pod from spec.podRef.
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

// reconcileHibernate writes HibernateState(true) onto the pod and
// probes epoch for the snapshot tag. On success the hibernation is
// marked Hibernated; otherwise the reconciler requeues. A probe
// error (transport failure, 5xx, auth) surfaces as a returned error
// so controller-runtime logs + requeues with backoff instead of
// silently retrying every requeueInterval.
func (r *Reconciler) reconcileHibernate(ctx context.Context, hib *cocoonv1.CocoonHibernation, pod *corev1.Pod, vmName string) (ctrl.Result, error) {
	if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, true); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch hibernate annotation: %w", err)
	}

	present, err := r.Epoch.HasManifest(ctx, vmName, meta.HibernateSnapshotTag)
	if err != nil {
		// HasManifest folds 404 into (false, nil); any error here
		// is transport / auth / 5xx and must surface so the CR
		// does not stay stuck at Hibernating.
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

// reconcileWake removes the hibernate annotation and waits for the
// pod's container to be running again.
func (r *Reconciler) reconcileWake(ctx context.Context, hib *cocoonv1.CocoonHibernation, pod *corev1.Pod, vmName string) (ctrl.Result, error) {
	logger := log.WithFunc("hibernation.Reconciler.reconcileWake")
	if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, false); err != nil {
		return ctrl.Result{}, fmt.Errorf("clear hibernate annotation: %w", err)
	}

	if meta.IsContainerRunning(pod) {
		// vk-cocoon has restored the VM. Drop the snapshot tag so a
		// future hibernate has a clean slate. A failure here is
		// non-fatal: a stale tag will be overwritten by the next
		// hibernate, and surfacing the error would block wake on a
		// transient registry hiccup.
		if err := r.Epoch.DeleteManifest(ctx, vmName, meta.HibernateSnapshotTag); err != nil {
			logger.Warnf(ctx, "delete hibernation snapshot %s: %v", vmName, err)
		}
		return ctrl.Result{}, r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseActive, vmName)
	}

	// Enforce the wake budget. The Ready condition's
	// LastTransitionTime is set the first time setPhase advances
	// into Waking and preserved on every subsequent no-op update,
	// so it survives controller restarts and gives us a stable
	// reference for how long we have been stuck.
	if wakeDeadlineExceeded(hib) {
		return ctrl.Result{}, r.markFailed(ctx, hib,
			fmt.Sprintf("wake timed out after %s; vk-cocoon never reported the container running", wakeTimeout))
	}

	if err := r.setPhase(ctx, hib, cocoonv1.CocoonHibernationPhaseWaking, vmName); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// wakeDeadlineExceeded reports whether the CocoonHibernation has
// been at Waking long enough that we should give up and fail the
// transition. It reads the transition timestamp from the Ready
// condition so the deadline is persisted in Status and survives
// operator restarts.
func wakeDeadlineExceeded(hib *cocoonv1.CocoonHibernation) bool {
	if hib.Status.Phase != cocoonv1.CocoonHibernationPhaseWaking {
		// Haven't even observed the Waking phase yet — the first
		// reconcile in this wake transition is free by definition.
		return false
	}
	ready := apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady)
	if ready == nil || ready.LastTransitionTime.IsZero() {
		return false
	}
	return time.Since(ready.LastTransitionTime.Time) > wakeTimeout
}

// setPhase patches the CocoonHibernation status with the supplied
// phase and uses apimeta.SetStatusCondition so the existing
// LastTransitionTime survives a no-op update. A previous Failed
// phase will be cleared as the recovery transition flows through.
//
// Failed→Waking re-entry needs a manual timestamp refresh:
// SetStatusCondition only bumps LastTransitionTime when the Ready
// Status field flips, and both Failed and Waking map to
// Ready=False. Without the override below, a recovered wake would
// inherit the stale timestamp from the previous failure and
// wakeDeadlineExceeded would trip immediately on the next
// reconcile, making recovery from a Failed wake impossible.
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

// markFailed marks the hibernation as Failed with a one-shot
// message. A subsequent pass through Reconcile that finds the
// failure preconditions cleared (e.g. the pod now exists) will land
// in setPhase and overwrite the Failed condition with the new
// in-flight state.
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

// readyCondition returns the Ready condition that mirrors a phase.
// LastTransitionTime is left zero so apimeta.SetStatusCondition
// preserves the existing timestamp on no-op updates.
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
