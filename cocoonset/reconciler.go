package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"
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

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/epoch"
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

// Reconciler reconciles a CocoonSet object.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Epoch  epoch.SnapshotRegistry
}

// SetupWithManager registers the reconciler against the supplied
// controller-runtime manager. The For predicate is
// GenerationChangedPredicate so reconciles only fire when the spec
// actually changes — status-only patches we make ourselves do not
// loop back. The Owns side keeps the pod-event firehose because
// pod status updates are exactly what drives the readyAgents diff.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cocoonv1.CocoonSet{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.Pod{}).
		Complete(r)
}

// Reconcile is the entry point invoked by controller-runtime each
// time a watched event lands.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.Reconcile")

	var cs cocoonv1.CocoonSet
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
	if classified.main != nil && meta.IsPodTerminal(classified.main) {
		return ctrl.Result{}, r.patchStatus(ctx, &cs,
			buildStatus(&cs, classified, cocoonv1.CocoonSetPhaseFailed))
	}

	// Suspend handling. We always ensure the main agent first
	// (suspend on a CocoonSet that never had a main pod would
	// otherwise produce a phantom Suspended phase with zero ready
	// agents). Once main exists we apply HibernateState(true) to
	// every owned pod and short-circuit the rest of the loop.
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

	// Un-suspend handling: if any owned pod still carries the
	// hibernate annotation from a prior Suspend=true pass, clear it
	// here before the ensure loops. Without this the pods remain
	// hibernated forever after Spec.Suspend flips back to false,
	// because vk-cocoon only wakes on a hibernate=false transition
	// and the annotation is only written by this reconciler.
	if err := r.applyUnsuspend(ctx, cs.Namespace, classified); err != nil {
		return ctrl.Result{}, err
	}

	// Ensure the main agent (slot 0) exists.
	if classified.main == nil {
		mainPod := buildAgentPod(&cs, 0, "", "", r.Scheme)
		if err := r.Create(ctx, mainPod); err != nil {
			return ctrl.Result{}, fmt.Errorf("create main agent: %w", err)
		}
		logger.Infof(ctx, "created main agent %s/%s", mainPod.Namespace, mainPod.Name)
		return ctrl.Result{Requeue: true}, nil
	}

	// Until the main agent is Ready we hold off on creating any
	// sub-agents — they fork from the main VM and need it to be live.
	if !meta.IsPodReady(classified.main) {
		return ctrl.Result{RequeueAfter: requeueWaitForMain},
			r.patchStatus(ctx, &cs, buildStatus(&cs, classified, cocoonv1.CocoonSetPhasePending))
	}

	mainVMName := meta.ParseVMSpec(classified.main).VMName
	mainNodeName := classified.main.Spec.NodeName

	// Track whether the ensure loops actually changed cluster
	// state. The status patch only needs a re-list when something
	// moved; otherwise the in-memory classified snapshot is fresh
	// enough and the next pod-event reconcile will pick up any
	// drift through the Owns watch.
	subChanged, err := r.ensureSubAgents(ctx, &cs, classified, mainVMName, mainNodeName)
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

	return ctrl.Result{}, r.patchStatus(ctx, &cs, buildStatus(&cs, classified, ""))
}

// ensureSubAgents creates missing sub-agent pods for slots
// [1..Replicas] and deletes any slot beyond Replicas. Returns true
// when at least one create or delete actually happened.
//
// IsAlreadyExists / IsNotFound from a previous reconcile race are
// suppressed but do not flip `changed`, so the success log only fires
// when we actually mutated cluster state.
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

// ensureToolboxes creates missing toolbox pods for every spec entry
// and deletes any toolbox not in spec. Returns true when at least
// one create or delete actually happened. Same suppress-but-don't-log
// rule as ensureSubAgents.
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

// reconcileDelete tears down everything the CocoonSet owns and then
// removes the finalizer so the API server can finalize the delete.
// Pod deletion and snapshot GC happen in a single pass — VM names are
// collected before each Delete call so we never need a second walk.
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

	// Per-pod GC decision mirrors meta.ShouldSnapshotVM so the delete
	// loop never issues a DeleteManifest against a tag vk-cocoon
	// never pushed. Under main-only that spares sub-agents and
	// toolboxes the 404-warn noise; under always/never the behavior
	// matches what the registry would return anyway.
	for i := range podList.Items {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctrl.Result{}, ctxErr
		}
		pod := &podList.Items[i]
		spec := meta.ParseVMSpec(pod)
		shouldGC := r.Epoch != nil && meta.ShouldSnapshotVM(spec)
		if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
			return ctrl.Result{}, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		if shouldGC && spec.VMName != "" {
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
// vk-cocoon picks up the annotation and snapshots / tears down the
// VM while keeping the container alive.
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

// applyUnsuspend clears HibernateState from every owned pod that
// still carries it, EXCEPT pods that are currently the target of an
// active CocoonHibernation CR with desire=Hibernate. Those have been
// hibernated by the per-pod path (not by spec.suspend) and clearing
// the annotation here would race the hibernation reconciler: vk-cocoon
// would wake the VM seconds after it finished snapshotting, the
// CocoonHibernation status would still read Hibernated, and the user
// would observe a phantom-running pod.
//
// The cheap path (no in-flight CRs in the namespace) only walks the
// owned pods and skips ones whose hibernate annotation is already
// absent — PatchHibernateState(false) is a no-op there.
func (r *Reconciler) applyUnsuspend(ctx context.Context, namespace string, classified classifiedPods) error {
	hibernatedByCR, err := r.podsHibernatedByCR(ctx, namespace)
	if err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(classified.allByName)) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		pod := classified.allByName[name]
		if !bool(meta.ReadHibernateState(pod)) {
			continue
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

// podsHibernatedByCR returns the set of pod names in `namespace` that
// are the target of a CocoonHibernation CR with desire=Hibernate.
// Used by applyUnsuspend to leave per-pod hibernations alone.
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
