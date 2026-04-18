package cocoonset

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/epoch"
)

const (
	finalizerName      = "cocoonset.cocoonstack.io/finalizer"
	requeueWaitForMain = 5 * time.Second
)

// Reconciler watches CocoonSet resources and manages the lifecycle of agent
// and toolbox pods to match the declared spec.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Epoch  epoch.SnapshotRegistry
}

// SetupWithManager registers the reconciler. For uses GenerationChangedPredicate
// to avoid status-update loops; Owns keeps pod events to drive readyAgents diffs.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cocoonv1.CocoonSet{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.Pod{}).
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
	if err := r.List(ctx, &podList,
		client.InNamespace(cs.Namespace),
		client.MatchingLabels{meta.LabelCocoonSet: cs.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list owned pods: %w", err)
	}

	// Filter out pods not owned by this CocoonSet to prevent stale-label
	// pods from being counted in status or affected by suspend/delete.
	owned := slices.DeleteFunc(podList.Items, func(p corev1.Pod) bool {
		return !metav1.IsControlledBy(&p, &cs)
	})
	classified := classifyPods(owned)

	// Stop reconciling if main agent is in a terminal phase (e.g. Failed).
	if classified.main != nil && meta.IsPodTerminal(classified.main) {
		return ctrl.Result{}, r.patchStatus(ctx, &cs,
			buildStatus(&cs, classified, cocoonv1.CocoonSetPhaseFailed))
	}

	// Suspend: ensure main exists first, then hibernate all owned pods.
	if cs.Spec.Suspend {
		if classified.main == nil {
			mainPod := buildAgentPod(&cs, 0, "", "", r.Scheme)
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
			buildStatus(&cs, classified, cocoonv1.CocoonSetPhaseSuspended))
	}

	// Clear stale hibernate annotations from a prior suspend pass.
	if err := r.applyUnsuspend(ctx, cs.Namespace, classified); err != nil {
		return ctrl.Result{}, err
	}

	if classified.main == nil {
		mainPod := buildAgentPod(&cs, 0, "", "", r.Scheme)
		if err := r.Create(ctx, mainPod); err != nil {
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

	subChanged, err := r.ensureSubAgents(ctx, &cs, classified, mainVMName, mainNodeName)
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

	return ctrl.Result{}, r.patchStatus(ctx, &cs, buildStatus(&cs, classified, ""))
}

// ensureSubAgents creates/deletes sub-agent pods to match [1..Replicas].
// Returns true when cluster state was mutated.
func (r *Reconciler) ensureSubAgents(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, mainVMName, mainNodeName string) (bool, error) {
	logger := log.WithFunc("cocoonset.Reconciler.ensureSubAgents")
	changed := false
	for slot := int32(1); slot <= cs.Spec.Agent.Replicas; slot++ {
		if _, exists := classified.sub[slot]; exists {
			continue
		}
		subPod := buildAgentPod(cs, slot, mainVMName, mainNodeName, r.Scheme)
		if err := r.Create(ctx, subPod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return changed, fmt.Errorf("create sub-agent slot %d: %w", slot, err)
		}
		logger.Infof(ctx, "created sub-agent %s/%s", subPod.Namespace, subPod.Name)
		changed = true
	}
	for _, slot := range slices.Sorted(maps.Keys(classified.sub)) {
		if slot <= cs.Spec.Agent.Replicas {
			continue
		}
		pod := classified.sub[slot]
		if err := r.Delete(ctx, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return changed, fmt.Errorf("delete extra sub-agent slot %d: %w", slot, err)
		}
		logger.Infof(ctx, "deleted extra sub-agent %s/%s", pod.Namespace, pod.Name)
		changed = true
	}
	return changed, nil
}

// ensureToolboxes creates/deletes toolbox pods to match spec.
// Returns true when cluster state was mutated.
func (r *Reconciler) ensureToolboxes(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, error) {
	logger := log.WithFunc("cocoonset.Reconciler.ensureToolboxes")
	changed := false
	desired := map[string]bool{}
	for _, tb := range cs.Spec.Toolboxes {
		desired[tb.Name] = true
		if _, exists := classified.toolbox[tb.Name]; exists {
			continue
		}
		tbPod := buildToolboxPod(cs, tb, r.Scheme)
		if err := r.Create(ctx, tbPod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return changed, fmt.Errorf("create toolbox %s: %w", tb.Name, err)
		}
		logger.Infof(ctx, "created toolbox %s/%s", tbPod.Namespace, tbPod.Name)
		changed = true
	}
	for _, name := range slices.Sorted(maps.Keys(classified.toolbox)) {
		if desired[name] {
			continue
		}
		pod := classified.toolbox[name]
		if err := r.Delete(ctx, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return changed, fmt.Errorf("delete extra toolbox %s: %w", name, err)
		}
		logger.Infof(ctx, "deleted extra toolbox %s/%s", pod.Namespace, pod.Name)
		changed = true
	}
	return changed, nil
}

// reconcileDelete deletes all owned pods, GCs snapshots, and removes the finalizer.
func (r *Reconciler) reconcileDelete(ctx context.Context, cs *cocoonv1.CocoonSet) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.reconcileDelete")
	logger.Infof(ctx, "deleting cocoonset %s/%s", cs.Namespace, cs.Name)

	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(cs.Namespace),
		client.MatchingLabels{meta.LabelCocoonSet: cs.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list owned pods for delete: %w", err)
	}

	// Phase 1: delete all pods and let vk-cocoon finish snapshot push.
	for i := range podList.Items {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctrl.Result{}, ctxErr
		}
		pod := &podList.Items[i]
		if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
			return ctrl.Result{}, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}

	// Requeue if any pods still exist — vk-cocoon's DeletePod may still
	// be running snapshot save/push. We only GC epoch tags and remove the
	// finalizer once every pod is fully gone from the API server.
	var remaining corev1.PodList
	if err := r.List(ctx, &remaining,
		client.InNamespace(cs.Namespace),
		client.MatchingLabels{meta.LabelCocoonSet: cs.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-list pods after delete: %w", err)
	}
	if len(remaining.Items) > 0 {
		logger.Infof(ctx, "waiting for %d pods to terminate before GC", len(remaining.Items))
		return ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
	}

	// All pods gone — safe to GC snapshot tags from epoch.
	if r.Epoch != nil {
		for i := range podList.Items {
			pod := &podList.Items[i]
			spec := meta.ParseVMSpec(pod)
			if !meta.ShouldSnapshotVM(spec) || spec.VMName == "" {
				continue
			}
			if err := r.Epoch.DeleteManifest(ctx, spec.VMName, meta.DefaultSnapshotTag); err != nil {
				logger.Warnf(ctx, "delete snapshot %s: %v", spec.VMName, err)
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
func (r *Reconciler) applySuspend(ctx context.Context, classified classifiedPods) error {
	for _, name := range slices.Sorted(maps.Keys(classified.allByName)) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		pod := classified.allByName[name]
		if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, true); err != nil {
			return fmt.Errorf("patch hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// applyUnsuspend clears HibernateState from owned pods, skipping pods that are
// targets of an active CocoonHibernation CR to avoid racing the hibernation reconciler.
func (r *Reconciler) applyUnsuspend(ctx context.Context, namespace string, classified classifiedPods) error {
	var hibernated []*corev1.Pod
	for _, pod := range classified.allByName {
		if bool(meta.ReadHibernateState(pod)) {
			hibernated = append(hibernated, pod)
		}
	}
	if len(hibernated) == 0 {
		return nil
	}
	slices.SortFunc(hibernated, func(a, b *corev1.Pod) int {
		return cmp.Compare(a.Name, b.Name)
	})

	hibernatedByCR, err := r.podsHibernatedByCR(ctx, namespace)
	if err != nil {
		return err
	}
	for _, pod := range hibernated {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if _, ownedByCR := hibernatedByCR[pod.Name]; ownedByCR {
			continue
		}
		if err := commonk8s.PatchHibernateState(ctx, r.Client, pod, false); err != nil {
			return fmt.Errorf("clear hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// podsHibernatedByCR returns pod names targeted by a desire=Hibernate CR.
func (r *Reconciler) podsHibernatedByCR(ctx context.Context, namespace string) (map[string]struct{}, error) {
	var hibList cocoonv1.CocoonHibernationList
	if err := r.List(ctx, &hibList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list cocoonhibernations in %s: %w", namespace, err)
	}
	out := make(map[string]struct{}, len(hibList.Items))
	for i := range hibList.Items {
		hib := &hibList.Items[i]
		if hib.Spec.Desire != cocoonv1.HibernationDesireHibernate {
			continue
		}
		if hib.Spec.PodRef.Name == "" {
			continue
		}
		out[hib.Spec.PodRef.Name] = struct{}{}
	}
	return out, nil
}
