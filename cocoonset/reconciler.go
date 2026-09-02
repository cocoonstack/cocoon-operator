package cocoonset

import (
	"cmp"
	"context"
	"fmt"
	"time"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/metrics"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

const (
	finalizerName      = "cocoonset.cocoonstack.io/finalizer"
	requeueWaitForMain = 5 * time.Second
	requeueSuspendPoll = 5 * time.Second
	requeueMigratePoll = 5 * time.Second
	requeueAfterWrite  = time.Second
)

// Reconciler drives agent and toolbox pods to match each CocoonSet spec.
type Reconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Registry  snapshot.Registry
	Recorder  record.EventRecorder
	// Concurrency caps in-flight reconciles; at 1, one slow registry probe stalls every other CocoonSet.
	Concurrency int
}

// SetupWithManager registers the reconciler with predicates that drop status-only churn.
func (r *Reconciler) SetupWithManager(_ context.Context, mgr ctrl.Manager) error {
	if r.Concurrency < 1 {
		return fmt.Errorf("cocoonset concurrency must be at least 1, got %d", r.Concurrency)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&cocoonv1.CocoonSet{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.Pod{}, builder.WithPredicates(podRelevantChange{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.Concurrency}).
		Complete(r)
}

// Reconcile drives one CocoonSet toward its declared agent and toolbox pods.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.Reconcile")

	var cs cocoonv1.CocoonSet
	if err := r.Get(ctx, req.NamespacedName, &cs); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get cocoonset %s: %w", req.NamespacedName, err)
	}

	if !cs.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &cs)
	}

	if controllerutil.AddFinalizer(&cs, finalizerName) {
		if err := r.Update(ctx, &cs); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeueAfterWrite}, nil
	}

	owned, listErr := r.listOwnedPods(ctx, &cs)
	if listErr != nil {
		return ctrl.Result{}, fmt.Errorf("list owned pods: %w", listErr)
	}
	classified := classifyPods(owned)

	// stamp before any spec-driven patch so observed-generation names the revision that produced the state
	if err := r.syncCocoonSetGeneration(ctx, &cs, classified); err != nil {
		return ctrl.Result{}, err
	}

	if cs.Spec.Suspend {
		if cs.Spec.HibernatePolicy.Default() == cocoonv1.HibernatePolicyRelease {
			return r.reconcileSuspendRelease(ctx, &cs, classified)
		}
		return r.reconcileSuspend(ctx, &cs, classified)
	}

	// lifecycle-state=Failed is the vk-cocoon terminal path and fires before Pod Phase flips; IsPodTerminal is the kubelet path
	if classified.main != nil {
		if reason := mainPodFailedReason(classified.main); reason != "" {
			return r.handleFailedMainAgent(ctx, &cs, classified, reason)
		}
		if cs.Status.Phase == cocoonv1.CocoonSetPhaseFailed && meta.IsPodReady(classified.main) {
			commonk8s.Eventf(r.Recorder, &cs, corev1.EventTypeNormal, "RecoveredFromFailure",
				"main pod %s/%s is Ready again", classified.main.Namespace, classified.main.Name)
		}
	}

	// migration runs before applyUnsuspend, which would otherwise clear its hibernate annotation
	if handled, res, err := r.reconcileMigration(ctx, &cs, classified); handled {
		return res, err
	}

	// wake runs before createMainAgent, whose CR-only restore intent would fresh-boot over the snapshot
	if handled, res, err := r.reconcileWake(ctx, &cs, classified); handled {
		return res, err
	}

	if err := r.applyUnsuspend(ctx, cs.Namespace, classified); err != nil {
		return ctrl.Result{}, err
	}

	if classified.main != nil && !podSpecMatchesAgent(classified.main, &cs, 0) {
		logger.Infof(ctx, "main agent %s/%s spec drifted, deleting for recreate", classified.main.Namespace, classified.main.Name)
		if err := r.Delete(ctx, classified.main); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete drifted main agent: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeueAfterWrite}, nil
	}
	intent := r.newRestoreIntent(ctx, cs.Namespace)
	if classified.main == nil {
		return r.createMainAgent(ctx, &cs, intent)
	}

	// sub-agents fork from main and need it live before creation
	if !meta.IsPodReady(classified.main) {
		return ctrl.Result{RequeueAfter: requeueWaitForMain},
			r.patchStatus(ctx, &cs, buildStatus(&cs, classified, cocoonv1.CocoonSetPhasePending))
	}

	mainVMName := meta.ParseVMSpec(classified.main).VMName
	mainNodeName := classified.main.Spec.NodeName

	subChanged, subRequeue, err := r.ensureSubAgents(ctx, &cs, classified, mainVMName, mainNodeName, intent)
	if err != nil {
		return ctrl.Result{}, err
	}
	tbChanged, err := r.ensureToolboxes(ctx, &cs, classified, intent)
	if err != nil {
		return ctrl.Result{}, err
	}

	if subChanged || tbChanged {
		return ctrl.Result{RequeueAfter: requeueAfterWrite}, nil
	}
	if err := r.patchStatus(ctx, &cs, buildStatus(&cs, classified, "")); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: subRequeue}, nil
}

// handleFailedMainAgent recreates a drifted terminal main; parked in Failed it would wait for a Ready it can never reach.
func (r *Reconciler) handleFailedMainAgent(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, reason string) (ctrl.Result, error) {
	if !podSpecMatchesAgent(classified.main, cs, 0) {
		if err := r.Delete(ctx, classified.main); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete terminal drifted main agent: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeueAfterWrite}, nil
	}
	r.observeMainPodFailed(cs, classified.main, reason)
	return ctrl.Result{}, r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseFailed))
}

// createMainAgent always requeues so sub-agents fork off the new main.
func (r *Reconciler) createMainAgent(ctx context.Context, cs *cocoonv1.CocoonSet, intent restoreIntent) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.createMainAgent")
	mainPod, err := buildAgentPod(cs, 0, "", "", r.Scheme)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("build main agent: %w", err)
	}
	if err := r.markRestoreFromIntent(ctx, mainPod, intent); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, mainPod); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// old pod still Terminating; requeue and wait
			return ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
		}
		return ctrl.Result{}, fmt.Errorf("create main agent: %w", err)
	}
	logger.Infof(ctx, "created main agent %s/%s", mainPod.Namespace, mainPod.Name)
	return ctrl.Result{RequeueAfter: requeueAfterWrite}, nil
}

// observeMainPodFailed bumps the lifecycle counter only on the annotation path so Pod-Phase failures do not dilute it.
func (r *Reconciler) observeMainPodFailed(cs *cocoonv1.CocoonSet, pod *corev1.Pod, reason string) {
	if reason == "PodLifecycleFailed" {
		phase := cmp.Or(string(cs.Status.Phase), string(cocoonv1.CocoonSetPhasePending))
		metrics.LifecycleStateFailedObservedTotal.WithLabelValues(phase).Inc()
	}
	msg := cmp.Or(pod.Annotations[meta.AnnotationLifecycleStateMessage], string(pod.Status.Phase))
	commonk8s.Eventf(r.Recorder, cs, corev1.EventTypeWarning, reason, "main pod %s/%s: %s", pod.Namespace, pod.Name, msg)
}

// mainPodFailedReason maps a terminal signal to its Failed Event reason; "" means not terminal.
func mainPodFailedReason(pod *corev1.Pod) string {
	if meta.ReadLifecycleState(pod) == meta.LifecycleStateFailed {
		return "PodLifecycleFailed"
	}
	if meta.IsPodTerminal(pod) {
		return "MainAgentFailed"
	}
	return ""
}

// podIsTerminal covers both the kubelet and the vk-cocoon-driven failure paths.
func podIsTerminal(pod *corev1.Pod) bool {
	return mainPodFailedReason(pod) != ""
}
