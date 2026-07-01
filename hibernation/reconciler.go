// Package hibernation drives hibernate/wake transitions for CocoonHibernation CRs.
package hibernation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/metrics"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

const (
	requeueInterval = 5 * time.Second
	// hibernateTimeout bounds how long Hibernating can last before marking Failed.
	hibernateTimeout = 3 * time.Minute
	// wakeTimeout bounds how long Waking can last before marking Failed.
	wakeTimeout = 5 * time.Minute

	// indexPodRefName keys CocoonHibernation objects by spec.podRef.name so the
	// pod watcher can resolve a pod event back to the CRs that target it.
	indexPodRefName = "spec.podRef.name"

	// finalizerName keeps the CR alive long enough to clear its :hibernate tag from the registry.
	finalizerName = "cocoonhibernation.cocoonset.cocoonstack.io/finalizer"

	conditionReasonPending = "Pending"
	conditionReasonDone    = "Done"
	conditionReasonFailed  = "Failed"
)

// Reconciler watches CocoonHibernation resources and drives hibernate/wake
// transitions by toggling pod annotations and polling the snapshot registry.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Registry snapshot.Registry
	Recorder record.EventRecorder

	// observed[UID] = last recorded Ready.LastTransitionTime, dedups
	// phase-exit observations against controller-runtime cache lag.
	observed sync.Map
}

// SetupWithManager registers the reconciler with the controller manager.
// An index on spec.podRef.name lets the pod watcher fan out events to every
// CR targeting a given pod, so late-arriving pods self-heal without user edits.
func (r *Reconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		ctx, &cocoonv1.CocoonHibernation{}, indexPodRefName,
		func(o client.Object) []string {
			return []string{o.(*cocoonv1.CocoonHibernation).Spec.PodRef.Name}
		},
	); err != nil {
		return fmt.Errorf("index %s: %w", indexPodRefName, err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&cocoonv1.CocoonHibernation{}).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.hibernationsTargetingPod),
			// Ignore status-only churn; we only care about creation, deletion,
			// and annotation changes (VMName arriving, hibernate flag toggling).
			builder.WithPredicates(predicate.Or(
				predicate.AnnotationChangedPredicate{},
				predicate.Funcs{
					CreateFunc: func(event.CreateEvent) bool { return true },
					DeleteFunc: func(event.DeleteEvent) bool { return true },
				},
			)),
		).
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

	if !hib.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &hib)
	}
	if !controllerutil.ContainsFinalizer(&hib, finalizerName) {
		controllerutil.AddFinalizer(&hib, finalizerName)
		if err := r.Update(ctx, &hib); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if hib.Spec.PodRef.Name == "" {
		return ctrl.Result{}, r.markFailed(ctx, &hib, "spec.podRef.name is required")
	}
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: hib.Namespace, Name: hib.Spec.PodRef.Name}, &pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Pod may arrive after the CR; a pod Create event will reconcile us
			// via Watches, but still requeue as a safety net.
			return ctrl.Result{RequeueAfter: requeueInterval}, r.markPending(ctx, &hib, fmt.Sprintf("pod %s/%s not yet present", hib.Namespace, hib.Spec.PodRef.Name))
		}
		return ctrl.Result{}, fmt.Errorf("get target pod: %w", err)
	}

	vmName := meta.ParseVMSpec(&pod).VMName
	if vmName == "" {
		// VMName is filled by vk-cocoon once the VM is provisioned; wait.
		return ctrl.Result{RequeueAfter: requeueInterval}, r.markPending(ctx, &hib, fmt.Sprintf("pod %s/%s has no %s annotation yet", pod.Namespace, pod.Name, meta.AnnotationVMName))
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

// reconcileDelete clears the :hibernate tag (if Status.VMName is set) and removes the finalizer.
func (r *Reconciler) reconcileDelete(ctx context.Context, hib *cocoonv1.CocoonHibernation) (ctrl.Result, error) {
	logger := log.WithFunc("hibernation.Reconciler.reconcileDelete")
	if r.Registry != nil && hib.Status.VMName != "" {
		if err := r.Registry.DeleteManifest(ctx, hib.Status.VMName, meta.HibernateSnapshotTag); err != nil {
			logger.Warnf(ctx, "delete hibernate snapshot %s: %v", hib.Status.VMName, err)
		}
	}
	r.observed.Delete(string(hib.UID))
	if controllerutil.ContainsFinalizer(hib, finalizerName) {
		controllerutil.RemoveFinalizer(hib, finalizerName)
		if err := r.Update(ctx, hib); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

// hibernationsTargetingPod returns reconcile requests for every CocoonHibernation
// whose PodRef points at the given pod. Called from the Pod watcher.
func (r *Reconciler) hibernationsTargetingPod(ctx context.Context, obj client.Object) []ctrl.Request {
	var list cocoonv1.CocoonHibernationList
	if err := r.List(
		ctx, &list,
		client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{indexPodRefName: obj.GetName()},
	); err != nil {
		log.WithFunc("hibernation.Reconciler.hibernationsTargetingPod").
			Warnf(ctx, "list hibernations targeting %s/%s: %v", obj.GetNamespace(), obj.GetName(), err)
		return nil
	}
	out := make([]ctrl.Request, 0, len(list.Items))
	for i := range list.Items {
		h := &list.Items[i]
		out = append(out, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: h.Namespace, Name: h.Name}})
	}
	return out
}

// setPhase patches status when phase, vmName, or generation moved. On
// Failed->Waking re-entry it also refreshes Ready.LastTransitionTime so the
// wake deadline doesn't carry over from the previous failure.
func (r *Reconciler) setPhase(ctx context.Context, hib *cocoonv1.CocoonHibernation, phase cocoonv1.CocoonHibernationPhase, vmName string) error {
	if hib.Status.Phase == phase && hib.Status.VMName == vmName && hib.Status.ObservedGeneration == hib.Generation {
		return nil
	}
	refreshDeadline := hasPhaseDeadline(phase) && hib.Status.Phase != phase
	if err := commonk8s.PatchStatus(ctx, r.Client, hib, func(h *cocoonv1.CocoonHibernation) {
		h.Status.ObservedGeneration = h.Generation
		h.Status.Phase = phase
		h.Status.VMName = vmName
		apimeta.SetStatusCondition(&h.Status.Conditions, readyCondition(phase, h.Generation))
		if refreshDeadline {
			if ready := apimeta.FindStatusCondition(h.Status.Conditions, commonk8s.ConditionTypeReady); ready != nil {
				ready.LastTransitionTime = metav1.Now()
			}
		}
	}); err != nil {
		return fmt.Errorf("patch hibernation status: %w", err)
	}
	return nil
}

// firstTransitionAt reports whether Ready.LastTransitionTime has advanced
// since the last observation for this CR.
func (r *Reconciler) firstTransitionAt(hib *cocoonv1.CocoonHibernation) bool {
	ready := apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady)
	if ready == nil || ready.LastTransitionTime.IsZero() {
		return false
	}
	key := string(hib.UID)
	got, loaded := r.observed.LoadOrStore(key, ready.LastTransitionTime.Time)
	if !loaded {
		return true
	}
	if ready.LastTransitionTime.After(got.(time.Time)) {
		r.observed.Store(key, ready.LastTransitionTime.Time)
		return true
	}
	return false
}

func (r *Reconciler) emitWarningf(hib *cocoonv1.CocoonHibernation, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(hib, corev1.EventTypeWarning, reason, format, args...)
	}
}

func (r *Reconciler) emitNormalf(hib *cocoonv1.CocoonHibernation, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(hib, corev1.EventTypeNormal, reason, format, args...)
	}
}

// announceRetryFromFailed emits a Normal event when a reconcile re-enters
// hibernate/wake after a prior Failed phase.
func (r *Reconciler) announceRetryFromFailed(hib *cocoonv1.CocoonHibernation, desire cocoonv1.HibernationDesire) {
	if hib.Status.Phase != cocoonv1.CocoonHibernationPhaseFailed {
		return
	}
	r.emitNormalf(hib, "RetryRequested", "retrying %s after prior failure", desire)
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

// markPending parks the CR on Pending without pinning Failed; self-heals on
// re-enqueue. Short-circuits when phase, generation, and Ready message all
// match so the pod watcher doesn't PATCH on every event.
func (r *Reconciler) markPending(ctx context.Context, hib *cocoonv1.CocoonHibernation, msg string) error {
	if hib.Status.Phase == cocoonv1.CocoonHibernationPhasePending && hib.Status.ObservedGeneration == hib.Generation {
		if ready := apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady); ready != nil && ready.Message == msg {
			return nil
		}
	}
	if err := commonk8s.PatchStatus(ctx, r.Client, hib, func(h *cocoonv1.CocoonHibernation) {
		h.Status.ObservedGeneration = h.Generation
		h.Status.Phase = cocoonv1.CocoonHibernationPhasePending
		apimeta.SetStatusCondition(&h.Status.Conditions, commonk8s.NewReadyCondition(
			h.Generation, metav1.ConditionFalse, conditionReasonPending, msg,
		))
	}); err != nil {
		return fmt.Errorf("patch pending status: %w", err)
	}
	return nil
}

// hasPhaseDeadline reports whether a phase carries a deadline that must reset
// on re-entry (so a Failed→Hibernating retry doesn't inherit the old clock).
func hasPhaseDeadline(p cocoonv1.CocoonHibernationPhase) bool {
	return p == cocoonv1.CocoonHibernationPhaseHibernating || p == cocoonv1.CocoonHibernationPhaseWaking
}

// phaseDeadlineExceeded reports whether hib has been in phase longer than timeout,
// measured from Ready.LastTransitionTime.
func phaseDeadlineExceeded(hib *cocoonv1.CocoonHibernation, phase cocoonv1.CocoonHibernationPhase, timeout time.Duration) bool {
	if hib.Status.Phase != phase {
		return false
	}
	ready := apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady)
	if ready == nil || ready.LastTransitionTime.IsZero() {
		return false
	}
	return time.Since(ready.LastTransitionTime.Time) > timeout
}

// observePhaseExit records the duration spent in the current phase. Call
// before transitioning away from Hibernating or Waking.
func observePhaseExit(hib *cocoonv1.CocoonHibernation, result string) {
	ready := apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady)
	if ready == nil || ready.LastTransitionTime.IsZero() {
		return
	}
	elapsed := time.Since(ready.LastTransitionTime.Time).Seconds()
	switch hib.Status.Phase {
	case cocoonv1.CocoonHibernationPhaseHibernating:
		metrics.HibernatePhaseDurationSeconds.WithLabelValues(result).Observe(elapsed)
	case cocoonv1.CocoonHibernationPhaseWaking:
		metrics.WakePhaseDurationSeconds.WithLabelValues(result).Observe(elapsed)
	}
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
