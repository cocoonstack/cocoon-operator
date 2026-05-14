package cocoonset

import (
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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/metrics"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

const (
	finalizerName      = "cocoonset.cocoonstack.io/finalizer"
	requeueWaitForMain = 5 * time.Second
	requeueSuspendPoll = 5 * time.Second
)

// Reconciler watches CocoonSet resources and manages the lifecycle of agent
// and toolbox pods to match the declared spec.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Epoch    snapshot.Registry
	Recorder record.EventRecorder
}

// SetupWithManager registers the reconciler. `For` uses GenerationChangedPredicate
// to avoid status-update loops; Owns filters pod events to creation, deletion,
// and readiness transitions to prevent reconcile storms from VK status churn.
func (r *Reconciler) SetupWithManager(_ context.Context, mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cocoonv1.CocoonSet{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.Pod{}, builder.WithPredicates(podRelevantChange{})).
		Complete(r)
}

// Reconcile drives a single CocoonSet toward its desired state by ensuring
// the correct set of agent and toolbox pods exist.
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

	if !controllerutil.ContainsFinalizer(&cs, finalizerName) {
		controllerutil.AddFinalizer(&cs, finalizerName)
		if err := r.Update(ctx, &cs); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	var podList corev1.PodList
	if err := r.List(
		ctx, &podList,
		client.InNamespace(cs.Namespace),
		client.MatchingLabels{meta.LabelCocoonSet: cs.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list owned pods: %w", err)
	}

	// Filter out pods not owned by this CocoonSet to prevent stale-label
	// pods from being counted in status or affected by suspend/delete.
	owned := filterOwnedPods(podList.Items, &cs)
	classified := classifyPods(owned)

	// Stamp before any spec-driven patch so observed-generation reflects
	// the spec revision that produced the resulting state.
	if err := r.syncCocoonSetGeneration(ctx, &cs, classified); err != nil {
		return ctrl.Result{}, err
	}

	// Stop reconciling if main agent is in a terminal state. lifecycle-state=Failed
	// is the vk-cocoon-driven path (terminal before Pod Phase flips); IsPodTerminal
	// is the kubelet-driven path. Both transition CocoonSet to Failed.
	if classified.main != nil {
		if reason := mainPodFailedReason(classified.main); reason != "" {
			r.observeMainPodFailed(&cs, classified.main, reason)
			return ctrl.Result{}, r.patchStatus(ctx, &cs,
				buildStatus(&cs, classified, cocoonv1.CocoonSetPhaseFailed))
		}
		if cs.Status.Phase == cocoonv1.CocoonSetPhaseFailed && meta.IsPodReady(classified.main) && r.Recorder != nil {
			r.Recorder.Eventf(&cs, corev1.EventTypeNormal, "RecoveredFromFailure",
				"main pod %s/%s is Ready again", classified.main.Namespace, classified.main.Name)
		}
	}

	if cs.Spec.Suspend {
		return r.reconcileSuspend(ctx, &cs, classified)
	}

	// Clear stale hibernate annotations from a prior suspend pass.
	if err := r.applyUnsuspend(ctx, cs.Namespace, classified); err != nil {
		return ctrl.Result{}, err
	}

	if classified.main != nil && !podSpecMatchesAgent(classified.main, &cs, 0) {
		logger.Infof(ctx, "main agent %s/%s spec drifted, deleting for recreate", classified.main.Namespace, classified.main.Name)
		if err := r.Delete(ctx, classified.main); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete drifted main agent: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if classified.main == nil {
		mainPod, err := buildAgentPod(&cs, 0, "", "", r.Scheme)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("build main agent: %w", err)
		}
		if err := r.Create(ctx, mainPod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Old pod still Terminating; requeue and wait.
				return ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
			}
			return ctrl.Result{}, fmt.Errorf("create main agent: %w", err)
		}
		logger.Infof(ctx, "created main agent %s/%s", mainPod.Namespace, mainPod.Name)
		return ctrl.Result{Requeue: true}, nil
	}

	// Sub-agents fork from main and need it live before creation.
	if !meta.IsPodReady(classified.main) {
		return ctrl.Result{RequeueAfter: requeueWaitForMain},
			r.patchStatus(ctx, &cs, buildStatus(&cs, classified, cocoonv1.CocoonSetPhasePending))
	}

	mainVMName := meta.ParseVMSpec(classified.main).VMName
	mainNodeName := classified.main.Spec.NodeName

	subChanged, subRequeue, err := r.ensureSubAgents(ctx, &cs, classified, mainVMName, mainNodeName)
	if err != nil {
		return ctrl.Result{}, err
	}
	tbChanged, err := r.ensureToolboxes(ctx, &cs, classified)
	if err != nil {
		return ctrl.Result{}, err
	}

	if subChanged || tbChanged {
		return ctrl.Result{Requeue: true}, nil
	}
	if err := r.patchStatus(ctx, &cs, buildStatus(&cs, classified, "")); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: subRequeue}, nil
}

// mainPodFailedReason maps a pod's terminal signal to the Event reason that
// the operator emits when transitioning the CocoonSet to Failed; "" means
// the pod is not terminal.
func mainPodFailedReason(pod *corev1.Pod) string {
	if meta.ReadLifecycleState(pod) == meta.LifecycleStateFailed {
		return "PodLifecycleFailed"
	}
	if meta.IsPodTerminal(pod) {
		return "MainAgentFailed"
	}
	return ""
}

// podIsTerminal reports whether pod is in a terminal state from either the
// kubelet (Pod.Status.Phase=Failed) or the vk-cocoon-driven path
// (vm.cocoonstack.io/lifecycle-state=Failed). Sub-agents and toolboxes use
// this to trigger rebuild for both signals; main pod uses mainPodFailedReason
// directly to differentiate the Event reason.
func podIsTerminal(pod *corev1.Pod) bool {
	return meta.IsPodTerminal(pod) || meta.ReadLifecycleState(pod) == meta.LifecycleStateFailed
}

// observeMainPodFailed records the failure on the event channel and, when the
// signal came from a vk-cocoon lifecycle annotation, bumps the dedicated
// counter so the Pod-Phase-only path doesn't dilute the metric's meaning.
func (r *Reconciler) observeMainPodFailed(cs *cocoonv1.CocoonSet, pod *corev1.Pod, reason string) {
	if reason == "PodLifecycleFailed" {
		phase := string(cs.Status.Phase)
		if phase == "" {
			phase = string(cocoonv1.CocoonSetPhasePending)
		}
		metrics.LifecycleStateFailedObservedTotal.WithLabelValues(phase).Inc()
	}
	if r.Recorder == nil {
		return
	}
	msg := pod.Annotations[meta.AnnotationLifecycleStateMessage]
	if msg == "" {
		msg = string(pod.Status.Phase)
	}
	r.Recorder.Eventf(cs, corev1.EventTypeWarning, reason, "main pod %s/%s: %s", pod.Namespace, pod.Name, msg)
}
