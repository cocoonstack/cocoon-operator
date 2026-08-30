// Package hibernation drives hibernate/wake transitions for CocoonHibernation CRs.
package hibernation

import (
	"context"
	"fmt"
	"hash/maphash"
	"slices"
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
	"sigs.k8s.io/controller-runtime/pkg/controller"
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

	// indexPodRefName lets the pod watcher resolve a pod event back to the CRs targeting it.
	indexPodRefName = "spec.podRef.name"

	// finalizerName keeps the CR alive long enough to clear its :hibernate tag from the registry.
	finalizerName = "cocoonhibernation.cocoonset.cocoonstack.io/finalizer"

	// vmLockStripes bounds the lock table; 64 keeps collisions rare at realistic per-namespace VM counts.
	vmLockStripes = 64

	conditionReasonPending = "Pending"
	conditionReasonDone    = "Done"
	conditionReasonFailed  = "Failed"
)

var vmLockSeed = maphash.MakeSeed()

// Reconciler drives hibernate/wake transitions for CocoonHibernation CRs.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Registry snapshot.Registry
	Recorder record.EventRecorder
	// Concurrency caps in-flight reconciles; at 1, one slow registry request stalls every other CR.
	Concurrency int

	// observed dedups phase-exit observations against controller-runtime cache lag, keyed by UID.
	observed sync.Map

	// vmLocks serializes CRs that reach one VM; above one worker their opposing desires would race the tag and annotation.
	vmLocks [vmLockStripes]sync.Mutex
}

// SetupWithManager indexes spec.podRef.name so pod events fan out to every CR targeting that pod.
func (r *Reconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if r.Concurrency < 1 {
		return fmt.Errorf("hibernation concurrency must be at least 1, got %d", r.Concurrency)
	}
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
			builder.WithPredicates(podWatchPredicate()),
		).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.Concurrency}).
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
		return ctrl.Result{}, r.reconcileDelete(ctx, &hib)
	}
	if controllerutil.AddFinalizer(&hib, finalizerName) {
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
			// a pod may arrive after the CR; the pod watcher reconciles us, the requeue is a safety net
			return ctrl.Result{RequeueAfter: requeueInterval}, r.markPending(ctx, &hib, fmt.Sprintf("pod %s/%s not yet present", hib.Namespace, hib.Spec.PodRef.Name))
		}
		return ctrl.Result{}, fmt.Errorf("get target pod: %w", err)
	}

	vmName := meta.ParseVMSpec(&pod).VMName
	if vmName == "" {
		// vk-cocoon fills VMName once the VM is provisioned; wait
		return ctrl.Result{RequeueAfter: requeueInterval}, r.markPending(ctx, &hib, fmt.Sprintf("pod %s/%s has no %s annotation yet", pod.Namespace, pod.Name, meta.AnnotationVMName))
	}
	defer r.lockVM(vmName)()

	logger.Debugf(ctx, "reconcile hibernation %s/%s desire=%s vm=%s", hib.Namespace, hib.Name, hib.Spec.Desire, vmName)

	// reverse desires serialize through the in-flight terminal phase; vk flips neither the tag nor the annotations until the transition ends
	switch hib.Spec.Desire {
	case cocoonv1.HibernationDesireHibernate:
		if hib.Status.Phase == cocoonv1.CocoonHibernationPhaseWaking {
			return r.reconcileWake(ctx, &hib, &pod, vmName)
		}
		return r.reconcileHibernate(ctx, &hib, &pod, vmName)
	case cocoonv1.HibernationDesireWake:
		if hib.Status.Phase == cocoonv1.CocoonHibernationPhaseHibernating {
			return r.reconcileHibernate(ctx, &hib, &pod, vmName)
		}
		return r.reconcileWake(ctx, &hib, &pod, vmName)
	default:
		return ctrl.Result{}, r.markFailed(ctx, &hib, fmt.Sprintf("unknown desire %q", hib.Spec.Desire))
	}
}

// lockVM stripes the table because VM names come and go; a collision only costs two VMs an occasional serialization.
func (r *Reconciler) lockVM(vmName string) func() {
	mu := &r.vmLocks[maphash.String(vmLockSeed, vmName)%vmLockStripes]
	mu.Lock()
	return mu.Unlock
}

// reconcileDelete clears the :hibernate tag (if Status.VMName is set) and removes the finalizer.
func (r *Reconciler) reconcileDelete(ctx context.Context, hib *cocoonv1.CocoonHibernation) error {
	logger := log.WithFunc("hibernation.Reconciler.reconcileDelete")
	// lock Status.VMName, the VM this CR actually owns; defense in depth for CRs retargeted before podRef became immutable
	if hib.Status.VMName != "" {
		defer r.lockVM(hib.Status.VMName)()
	}
	if r.Registry != nil && hib.Status.VMName != "" {
		held, err := r.vmHeldByAnotherCR(ctx, hib)
		if err != nil {
			return err
		}
		if held {
			logger.Infof(ctx, "keeping hibernate snapshot %s: another live CocoonHibernation still tracks it", hib.Status.VMName)
		} else if err := snapshot.DeleteManifestIfPresent(ctx, r.Registry, hib.Status.VMName, meta.HibernateSnapshotTag); err != nil {
			logger.Errorf(ctx, err, "delete hibernate snapshot %s", hib.Status.VMName)
		}
	}
	r.observed.Delete(string(hib.UID))
	if controllerutil.RemoveFinalizer(hib, finalizerName) {
		if err := r.Update(ctx, hib); err != nil {
			return fmt.Errorf("remove finalizer: %w", err)
		}
	}
	return nil
}

// vmHeldByAnotherCR lets the last holder GC the shared tag; deleting earlier would strand survivors on a tag-less Hibernated.
func (r *Reconciler) vmHeldByAnotherCR(ctx context.Context, hib *cocoonv1.CocoonHibernation) (bool, error) {
	var list cocoonv1.CocoonHibernationList
	if err := r.List(ctx, &list, client.InNamespace(hib.Namespace)); err != nil {
		return false, fmt.Errorf("list cocoonhibernations in %s: %w", hib.Namespace, err)
	}
	return slices.ContainsFunc(list.Items, func(o cocoonv1.CocoonHibernation) bool {
		if o.UID == hib.UID || !o.DeletionTimestamp.IsZero() {
			return false
		}
		// PodRef also matches: a fresh duplicate holds the VM before its first reconcile writes Status.VMName
		return o.Status.VMName == hib.Status.VMName || o.Spec.PodRef.Name == hib.Spec.PodRef.Name
	}), nil
}

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
	out := make([]ctrl.Request, len(list.Items))
	for i := range list.Items {
		out[i] = ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])}
	}
	return out
}

// setPhase refreshes Ready.LastTransitionTime on phase entry so a deadline never carries over from a prior failure.
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

// firstTransitionAt reports whether Ready.LastTransitionTime advanced since the last observation.
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

func (r *Reconciler) emitEventf(hib *cocoonv1.CocoonHibernation, eventType, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(hib, eventType, reason, format, args...)
	}
}

// announcePhaseExitf observes the phase-duration exit and emits its event, once per Ready transition.
func (r *Reconciler) announcePhaseExitf(hib *cocoonv1.CocoonHibernation, result, eventType, reason, format string, args ...any) {
	if !r.firstTransitionAt(hib) {
		return
	}
	observePhaseExit(hib, result)
	r.emitEventf(hib, eventType, reason, format, args...)
}

func (r *Reconciler) announceRetryFromFailed(hib *cocoonv1.CocoonHibernation, desire cocoonv1.HibernationDesire) {
	if hib.Status.Phase != cocoonv1.CocoonHibernationPhaseFailed {
		return
	}
	r.emitEventf(hib, corev1.EventTypeNormal, "RetryRequested", "retrying %s after prior failure", desire)
}

// markFailed sets the Failed phase. A subsequent reconcile can recover by overwriting it.
func (r *Reconciler) markFailed(ctx context.Context, hib *cocoonv1.CocoonHibernation, msg string) error {
	return r.patchNotReady(ctx, hib, cocoonv1.CocoonHibernationPhaseFailed, conditionReasonFailed, msg)
}

// markPending never demotes Hibernated or Waking: they carry cocoonset's restore intent, and losing it would fresh-boot a recreate.
func (r *Reconciler) markPending(ctx context.Context, hib *cocoonv1.CocoonHibernation, msg string) error {
	if hib.Status.Phase == cocoonv1.CocoonHibernationPhaseHibernated || hib.Status.Phase == cocoonv1.CocoonHibernationPhaseWaking {
		return nil
	}
	if hib.Status.Phase == cocoonv1.CocoonHibernationPhasePending && hib.Status.ObservedGeneration == hib.Generation {
		if ready := apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady); ready != nil && ready.Message == msg {
			return nil
		}
	}
	return r.patchNotReady(ctx, hib, cocoonv1.CocoonHibernationPhasePending, conditionReasonPending, msg)
}

func (r *Reconciler) patchNotReady(ctx context.Context, hib *cocoonv1.CocoonHibernation, phase cocoonv1.CocoonHibernationPhase, reason, msg string) error {
	if err := commonk8s.PatchStatus(ctx, r.Client, hib, func(h *cocoonv1.CocoonHibernation) {
		h.Status.ObservedGeneration = h.Generation
		h.Status.Phase = phase
		apimeta.SetStatusCondition(&h.Status.Conditions, commonk8s.NewReadyCondition(h.Generation, metav1.ConditionFalse, reason, msg))
	}); err != nil {
		return fmt.Errorf("patch %s status: %w", phase, err)
	}
	return nil
}

// hasPhaseDeadline marks phases whose deadline resets on re-entry so a retry does not inherit the old clock.
// podWatchPredicate admits creation, deletion, and annotation changes; status churn is left to the requeue poll.
func podWatchPredicate() predicate.Predicate {
	return predicate.Or(
		predicate.AnnotationChangedPredicate{},
		predicate.Funcs{
			CreateFunc: func(event.CreateEvent) bool { return true },
			DeleteFunc: func(event.DeleteEvent) bool { return true },
			UpdateFunc: func(event.UpdateEvent) bool { return false },
		},
	)
}

func hasPhaseDeadline(p cocoonv1.CocoonHibernationPhase) bool {
	return p == cocoonv1.CocoonHibernationPhaseHibernating || p == cocoonv1.CocoonHibernationPhaseWaking
}

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
