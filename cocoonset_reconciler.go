package main

import (
	"context"
	"fmt"
	"time"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cocoonv1alpha1 "github.com/cocoonstack/cocoon-common/apis/v1alpha1"
	"github.com/cocoonstack/cocoon-common/meta"
)

const (
	// finalizerName is added to every CocoonSet so the reconciler
	// gets a chance to delete owned pods (and optionally garbage
	// collect snapshots) before the API server actually removes the
	// object.
	finalizerName = "cocoonset.cocoonstack.io/finalizer"

	// requeueWaitForMain is how long the reconciler waits before
	// re-checking whether the main agent has come up. The wait is
	// short on purpose: scaling sub-agents is gated on the main
	// agent being Ready.
	requeueWaitForMain = 5 * time.Second
)

// CocoonSetReconciler reconciles a CocoonSet object.
type CocoonSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Epoch  SnapshotRegistry
}

// SetupWithManager registers the reconciler against the supplied
// controller-runtime manager. The For predicate is
// GenerationChangedPredicate so reconciles only fire when the spec
// actually changes — status-only patches we make ourselves do not
// loop back. The Owns side keeps the pod-event firehose because
// pod status updates are exactly what drives the readyAgents diff.
func (r *CocoonSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cocoonv1alpha1.CocoonSet{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.Pod{}).
		Complete(r)
}

// Reconcile is the entry point invoked by controller-runtime each
// time a watched event lands.
func (r *CocoonSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.WithFunc("CocoonSetReconciler.Reconcile")

	var cs cocoonv1alpha1.CocoonSet
	if err := r.Get(ctx, req.NamespacedName, &cs); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get cocoonset %s: %w", req.NamespacedName, err)
	}

	// Deletion path: drop owned resources, then remove the finalizer.
	if !cs.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &cs)
	}

	// Ensure the finalizer is in place before doing anything else.
	if !controllerutil.ContainsFinalizer(&cs, finalizerName) {
		controllerutil.AddFinalizer(&cs, finalizerName)
		if err := r.Update(ctx, &cs); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// List every pod the operator owns for this CocoonSet by label.
	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(cs.Namespace),
		client.MatchingLabels{meta.LabelCocoonSet: cs.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list owned pods: %w", err)
	}

	classified := classifyPods(podList.Items)

	// Detect a wedged main agent (terminal phase like Failed) and
	// stop the reconcile loop with a Failed phase. Without this we
	// would requeue every 5 seconds forever, hiding the wedge from
	// status consumers.
	if classified.main != nil && isPodTerminal(classified.main) {
		return ctrl.Result{}, r.patchStatus(ctx, &cs,
			buildStatus(&cs, classified, cocoonv1alpha1.CocoonSetPhaseFailed))
	}

	// Suspend handling. We always ensure the main agent first
	// (suspend on a CocoonSet that never had a main pod would
	// otherwise produce a phantom Suspended phase with zero ready
	// agents). Once main exists we apply HibernateState(true) to
	// every owned pod and short-circuit the rest of the loop.
	if cs.Spec.Suspend {
		if classified.main == nil {
			mainPod := buildAgentPod(&cs, 0, "", r.Scheme)
			if err := r.Create(ctx, mainPod); err != nil {
				return ctrl.Result{}, fmt.Errorf("create main agent before suspend: %w", err)
			}
			logger.Infof(ctx, "created main agent %s/%s ahead of suspend", mainPod.Namespace, mainPod.Name)
			return ctrl.Result{Requeue: true}, nil
		}
		if err := r.applySuspend(ctx, classified); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.patchStatus(ctx, &cs,
			buildStatus(&cs, classified, cocoonv1alpha1.CocoonSetPhaseSuspended))
	}

	// Ensure the main agent (slot 0) exists.
	if classified.main == nil {
		mainPod := buildAgentPod(&cs, 0, "", r.Scheme)
		if err := r.Create(ctx, mainPod); err != nil {
			return ctrl.Result{}, fmt.Errorf("create main agent: %w", err)
		}
		logger.Infof(ctx, "created main agent %s/%s", mainPod.Namespace, mainPod.Name)
		return ctrl.Result{Requeue: true}, nil
	}

	// Until the main agent is Ready we hold off on creating any
	// sub-agents — they fork from the main VM and need it to be live.
	if !isPodReady(classified.main) {
		return ctrl.Result{RequeueAfter: requeueWaitForMain},
			r.patchStatus(ctx, &cs, buildStatus(&cs, classified, cocoonv1alpha1.CocoonSetPhasePending))
	}

	mainVMName := meta.ParseVMSpec(classified.main).VMName

	// Track whether the ensure loops actually changed cluster
	// state. The status patch only needs a re-list when something
	// moved; otherwise the in-memory classified snapshot is fresh
	// enough and the next pod-event reconcile will pick up any
	// drift through the Owns watch.
	subChanged, err := r.ensureSubAgents(ctx, &cs, classified, mainVMName)
	if err != nil {
		return ctrl.Result{}, err
	}
	tbChanged, err := r.ensureToolboxes(ctx, &cs, classified)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Skip the second list when no creates / deletes happened.
	// The Owns watch on Pod will fire a fresh reconcile as soon as
	// the API server commits any of our writes anyway.
	if subChanged || tbChanged {
		return ctrl.Result{Requeue: true}, nil
	}

	return ctrl.Result{}, r.patchStatus(ctx, &cs, buildStatus(&cs, classified, currentPhase(&cs, classified)))
}

// ensureSubAgents creates missing sub-agent pods for slots
// [1..Replicas] and deletes any slot beyond Replicas. Returns true
// when at least one create or delete actually happened.
func (r *CocoonSetReconciler) ensureSubAgents(ctx context.Context, cs *cocoonv1alpha1.CocoonSet, classified classifiedPods, mainVMName string) (bool, error) {
	logger := log.WithFunc("CocoonSetReconciler.ensureSubAgents")
	changed := false
	for slot := int32(1); slot <= cs.Spec.Agent.Replicas; slot++ {
		if _, exists := classified.sub[slot]; exists {
			continue
		}
		subPod := buildAgentPod(cs, slot, mainVMName, r.Scheme)
		if err := r.Create(ctx, subPod); err != nil && !apierrors.IsAlreadyExists(err) {
			return changed, fmt.Errorf("create sub-agent slot %d: %w", slot, err)
		}
		logger.Infof(ctx, "created sub-agent %s/%s", subPod.Namespace, subPod.Name)
		changed = true
	}
	for slot, pod := range classified.sub {
		if slot <= cs.Spec.Agent.Replicas {
			continue
		}
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return changed, fmt.Errorf("delete extra sub-agent slot %d: %w", slot, err)
		}
		logger.Infof(ctx, "deleted extra sub-agent %s/%s", pod.Namespace, pod.Name)
		changed = true
	}
	return changed, nil
}

// ensureToolboxes creates missing toolbox pods for every spec entry
// and deletes any toolbox not in spec. Returns true when at least
// one create or delete actually happened.
func (r *CocoonSetReconciler) ensureToolboxes(ctx context.Context, cs *cocoonv1alpha1.CocoonSet, classified classifiedPods) (bool, error) {
	logger := log.WithFunc("CocoonSetReconciler.ensureToolboxes")
	changed := false
	desired := map[string]bool{}
	for _, tb := range cs.Spec.Toolboxes {
		desired[tb.Name] = true
		if _, exists := classified.toolbox[tb.Name]; exists {
			continue
		}
		tbPod := buildToolboxPod(cs, tb, r.Scheme)
		if err := r.Create(ctx, tbPod); err != nil && !apierrors.IsAlreadyExists(err) {
			return changed, fmt.Errorf("create toolbox %s: %w", tb.Name, err)
		}
		logger.Infof(ctx, "created toolbox %s/%s", tbPod.Namespace, tbPod.Name)
		changed = true
	}
	for name, pod := range classified.toolbox {
		if desired[name] {
			continue
		}
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return changed, fmt.Errorf("delete extra toolbox %s: %w", name, err)
		}
		logger.Infof(ctx, "deleted extra toolbox %s/%s", pod.Namespace, pod.Name)
		changed = true
	}
	return changed, nil
}

// reconcileDelete tears down everything the CocoonSet owns and then
// removes the finalizer so the API server can finalize the delete.
func (r *CocoonSetReconciler) reconcileDelete(ctx context.Context, cs *cocoonv1alpha1.CocoonSet) (ctrl.Result, error) {
	logger := log.WithFunc("CocoonSetReconciler.reconcileDelete")
	logger.Infof(ctx, "deleting cocoonset %s/%s", cs.Namespace, cs.Name)

	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(cs.Namespace),
		client.MatchingLabels{meta.LabelCocoonSet: cs.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list owned pods for delete: %w", err)
	}
	for i := range podList.Items {
		pod := &podList.Items[i]
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}

	// Garbage-collect snapshots when the policy says we should.
	if cs.Spec.SnapshotPolicy.Default() != cocoonv1alpha1.SnapshotPolicyNever && r.Epoch != nil {
		for i := range podList.Items {
			vmName := meta.ParseVMSpec(&podList.Items[i]).VMName
			if vmName == "" {
				continue
			}
			if err := r.Epoch.DeleteManifest(ctx, vmName, "latest"); err != nil {
				logger.Warnf(ctx, "delete snapshot %s: %v", vmName, err)
			}
		}
	}

	if controllerutil.ContainsFinalizer(cs, finalizerName) {
		controllerutil.RemoveFinalizer(cs, finalizerName)
		if err := r.Update(ctx, cs); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

// applySuspend writes HibernateState(true) onto every owned pod.
// vk-cocoon picks up the annotation and snapshots / tears down the
// VM while keeping the container alive.
func (r *CocoonSetReconciler) applySuspend(ctx context.Context, classified classifiedPods) error {
	for _, pod := range classified.allByName {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if meta.ReadHibernateState(pod) {
			continue
		}
		patch := client.MergeFrom(pod.DeepCopy())
		meta.HibernateState(true).Apply(pod)
		if err := r.Patch(ctx, pod, patch); err != nil {
			return fmt.Errorf("patch hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// isPodReady returns true if the pod has a Ready condition set to
// True. The reconciler uses this to gate sub-agent creation on the
// main agent's liveness.
func isPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// isPodTerminal reports whether the pod has reached a phase that
// will not progress without operator intervention. The reconciler
// surfaces this as CocoonSetPhaseFailed so users see the wedge in
// status instead of an indefinite Pending.
func isPodTerminal(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return pod.Status.Phase == corev1.PodFailed
}
