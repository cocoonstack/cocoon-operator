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

// annotationDeleteVMNames records VM names for the post-pod GC step, so a
// CocoonSet deleted before Status.Agents was patched still gets every tag cleaned.
const annotationDeleteVMNames = "cocoonset.cocoonstack.io/delete-vm-names"

func (r *Reconciler) reconcileDelete(ctx context.Context, cs *cocoonv1.CocoonSet) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.reconcileDelete")
	logger.Infof(ctx, "deleting cocoonset %s/%s", cs.Namespace, cs.Name)

	owned, listErr := r.listOwnedPods(ctx, cs)
	if listErr != nil {
		return ctrl.Result{}, fmt.Errorf("list owned pods for delete: %w", listErr)
	}

	// Stash VM names from live pods + Status before pods disappear.
	if err := r.stashDeleteVMNames(ctx, cs, owned); err != nil {
		return ctrl.Result{}, fmt.Errorf("stash vm names: %w", err)
	}

	// vk-cocoon completes the snapshot push during the grace period before GC.
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
	// snapshot save/push. We only GC registry tags once every pod is fully gone.
	remainingOwned, listErr := r.listOwnedPods(ctx, cs)
	if listErr != nil {
		return ctrl.Result{}, fmt.Errorf("re-list pods after delete: %w", listErr)
	}
	if len(remainingOwned) > 0 {
		logger.Infof(ctx, "waiting for %d pods to terminate before GC", len(remainingOwned))
		return ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
	}

	// :hibernate is always orphaned at teardown — drop unconditionally. :latest
	// is kept when shouldKeepLatestTag says vk-cocoon pushed it for retag.
	if r.Registry != nil {
		for _, name := range vmNamesForGC(cs) {
			// Non-fatal, but log at error: a persistent delete failure (e.g. the
			// registry SA lacking delete permission) silently leaks snapshots.
			if err := r.Registry.DeleteManifest(ctx, name, meta.HibernateSnapshotTag); err != nil {
				logger.Errorf(ctx, err, "delete snapshot %s:%s", name, meta.HibernateSnapshotTag)
			}
			if shouldKeepLatestTag(cs, name) {
				continue
			}
			if err := r.Registry.DeleteManifest(ctx, name, meta.DefaultSnapshotTag); err != nil {
				logger.Errorf(ctx, err, "delete snapshot %s:%s", name, meta.DefaultSnapshotTag)
			}
		}
	} else {
		logger.Warnf(ctx, "skipping registry tag GC for cocoonset %s/%s: registry not configured", cs.Namespace, cs.Name)
	}

	if controllerutil.RemoveFinalizer(cs, finalizerName) {
		if err := r.Update(ctx, cs); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

// stashDeleteVMNames merges VM names from Status, the previously stashed
// annotation, and live pods, then re-writes the annotation if anything changed.
func (r *Reconciler) stashDeleteVMNames(ctx context.Context, cs *cocoonv1.CocoonSet, owned []corev1.Pod) error {
	have := make(map[string]struct{})
	for _, n := range statusVMNames(cs) {
		have[n] = struct{}{}
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
	names := statusVMNames(cs)
	slices.Sort(names)
	return names
}

// statusVMNames collects the non-empty VM names recorded in the CocoonSet
// status, agents first then toolboxes, preserving insertion order.
func statusVMNames(cs *cocoonv1.CocoonSet) []string {
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

// shouldKeepLatestTag has only (CocoonSet, vmName) — no Pod/VMSpec — so it
// derives the role via meta.ExtractAgentSlot directly instead of meta.ShouldSnapshotVM.
func shouldKeepLatestTag(cs *cocoonv1.CocoonSet, vmName string) bool {
	switch cs.Spec.SnapshotPolicy.Default() {
	case cocoonv1.SnapshotPolicyNever:
		return false
	case cocoonv1.SnapshotPolicyMainOnly:
		return meta.ExtractAgentSlot(cs.Namespace, cs.Name, vmName) == 0
	default:
		return true
	}
}

func parseVMNamesAnnotation(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
