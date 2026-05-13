package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// annotationDeleteVMNames durably records VM names for the GC step that runs
// after pods are gone, so a CocoonSet deleted before Status.Agents was patched
// (race window between r.Create(pod) and the reconcile that fills status) still
// gets every tag cleaned. Annotation, not status, because it must survive the
// pod-still-terminating requeue without depending on a second-pass status write.
const annotationDeleteVMNames = "cocoonset.cocoonstack.io/delete-vm-names"

// reconcileDelete deletes all owned pods, GCs snapshots, and removes the finalizer.
func (r *Reconciler) reconcileDelete(ctx context.Context, cs *cocoonv1.CocoonSet) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.reconcileDelete")
	logger.Infof(ctx, "deleting cocoonset %s/%s", cs.Namespace, cs.Name)

	var podList corev1.PodList
	if err := r.List(
		ctx, &podList,
		client.InNamespace(cs.Namespace),
		client.MatchingLabels{meta.LabelCocoonSet: cs.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list owned pods for delete: %w", err)
	}
	owned := filterOwnedPods(podList.Items, cs)

	// Stash VM names from live pods + Status before pods disappear.
	if err := r.stashDeleteVMNames(ctx, cs, owned); err != nil {
		return ctrl.Result{}, fmt.Errorf("stash vm names: %w", err)
	}

	// Phase 1: delete all pods and let vk-cocoon finish snapshot push.
	for i := range owned {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctrl.Result{}, ctxErr
		}
		pod := &owned[i]
		if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
			return ctrl.Result{}, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}

	// Requeue if any pods still exist — vk-cocoon's DeletePod may still be running
	// snapshot save/push. We only GC epoch tags once every pod is fully gone.
	var remaining corev1.PodList
	if err := r.List(
		ctx, &remaining,
		client.InNamespace(cs.Namespace),
		client.MatchingLabels{meta.LabelCocoonSet: cs.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-list pods after delete: %w", err)
	}
	remainingOwned := filterOwnedPods(remaining.Items, cs)
	if len(remainingOwned) > 0 {
		logger.Infof(ctx, "waiting for %d pods to terminate before GC", len(remainingOwned))
		return ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
	}

	// :hibernate is pushed regardless of snapshotPolicy, so drop both tags unconditionally.
	if r.Epoch != nil {
		for _, name := range vmNamesForGC(cs) {
			for _, tag := range []string{meta.DefaultSnapshotTag, meta.HibernateSnapshotTag} {
				if err := r.Epoch.DeleteManifest(ctx, name, tag); err != nil {
					logger.Warnf(ctx, "delete snapshot %s:%s: %v", name, tag, err)
				}
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

// stashDeleteVMNames merges VM names from Status, live pods, and any previously
// stashed annotation, then re-writes the annotation if anything changed.
func (r *Reconciler) stashDeleteVMNames(ctx context.Context, cs *cocoonv1.CocoonSet, owned []corev1.Pod) error {
	have := make(map[string]struct{})
	for _, a := range cs.Status.Agents {
		if a.VMName != "" {
			have[a.VMName] = struct{}{}
		}
	}
	for _, tb := range cs.Status.Toolboxes {
		if tb.VMName != "" {
			have[tb.VMName] = struct{}{}
		}
	}
	for _, n := range parseVMNamesAnnotation(cs.Annotations[annotationDeleteVMNames]) {
		have[n] = struct{}{}
	}
	for i := range owned {
		if n := meta.ParseVMSpec(&owned[i]).VMName; n != "" {
			have[n] = struct{}{}
		}
	}
	if len(have) == 0 {
		return nil
	}
	names := slices.Sorted(maps.Keys(have))
	joined := strings.Join(names, ",")
	if cs.Annotations[annotationDeleteVMNames] == joined {
		return nil
	}
	patch := client.MergeFrom(cs.DeepCopy())
	if cs.Annotations == nil {
		cs.Annotations = map[string]string{}
	}
	cs.Annotations[annotationDeleteVMNames] = joined
	return r.Patch(ctx, cs, patch)
}

// vmNamesForGC returns the canonical GC list — read from the stashed annotation,
// falling back to Status when the annotation is somehow missing.
func vmNamesForGC(cs *cocoonv1.CocoonSet) []string {
	if names := parseVMNamesAnnotation(cs.Annotations[annotationDeleteVMNames]); len(names) > 0 {
		return names
	}
	names := make([]string, 0, len(cs.Status.Agents)+len(cs.Status.Toolboxes))
	for _, a := range cs.Status.Agents {
		if a.VMName != "" {
			names = append(names, a.VMName)
		}
	}
	for _, tb := range cs.Status.Toolboxes {
		if tb.VMName != "" {
			names = append(names, tb.VMName)
		}
	}
	return names
}

func parseVMNamesAnnotation(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
