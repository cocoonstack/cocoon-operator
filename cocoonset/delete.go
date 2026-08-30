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
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

// annotationDeleteVMNames survives a CocoonSet deleted before Status.Agents was ever patched.
const annotationDeleteVMNames = "cocoonset.cocoonstack.io/delete-vm-names"

func (r *Reconciler) reconcileDelete(ctx context.Context, cs *cocoonv1.CocoonSet) (ctrl.Result, error) {
	logger := log.WithFunc("cocoonset.Reconciler.reconcileDelete")
	logger.Infof(ctx, "deleting cocoonset %s/%s", cs.Namespace, cs.Name)

	owned, listErr := r.listOwnedPods(ctx, cs)
	if listErr != nil {
		return ctrl.Result{}, fmt.Errorf("list owned pods for delete: %w", listErr)
	}

	// stash VM names from live pods and Status before the pods disappear
	if err := r.stashDeleteVMNames(ctx, cs, owned); err != nil {
		return ctrl.Result{}, fmt.Errorf("stash vm names: %w", err)
	}

	// vk-cocoon completes the snapshot push during the grace period before GC
	for i := range owned {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctrl.Result{}, ctxErr
		}
		pod := &owned[i]
		if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
			return ctrl.Result{}, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}

	// GC registry tags only once every pod is gone; vk-cocoon's DeletePod may still be pushing the snapshot
	remainingOwned, listErr := r.listOwnedPods(ctx, cs)
	if listErr != nil {
		return ctrl.Result{}, fmt.Errorf("re-list pods after delete: %w", listErr)
	}
	if len(remainingOwned) > 0 {
		logger.Infof(ctx, "waiting for %d pods to terminate before GC", len(remainingOwned))
		return ctrl.Result{RequeueAfter: requeueWaitForMain}, nil
	}

	// :hibernate is always orphaned at teardown; :latest is kept when vk-cocoon pushed it for retag
	if r.Registry != nil {
		for _, name := range parseVMNamesAnnotation(cs.Annotations[annotationDeleteVMNames]) {
			// non-fatal but logged at error: a persistent delete failure silently leaks snapshots
			if err := r.deleteManifestIfPresent(ctx, name, meta.HibernateSnapshotTag); err != nil {
				logger.Errorf(ctx, err, "delete snapshot %s:%s", name, meta.HibernateSnapshotTag)
			}
			if shouldKeepLatestTag(cs, name) {
				continue
			}
			if err := r.deleteManifestIfPresent(ctx, name, meta.DefaultSnapshotTag); err != nil {
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

// deleteManifestIfPresent probes first: some registries materialize an empty repository while authorizing a DELETE for a missing tag.
func (r *Reconciler) deleteManifestIfPresent(ctx context.Context, name, reference string) error {
	present, err := r.Registry.HasManifest(ctx, name, reference)
	if err != nil {
		return fmt.Errorf("probe snapshot %s:%s: %w", name, reference, err)
	}
	if !present {
		return nil
	}
	return r.Registry.DeleteManifest(ctx, name, reference)
}

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
	return commonk8s.Patch(ctx, r.Client, cs, func(c *cocoonv1.CocoonSet) {
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations[annotationDeleteVMNames] = joined
	})
}

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

// shouldKeepLatestTag has no Pod to hand meta.ShouldSnapshotVM, so it derives the role via meta.ExtractAgentSlot.
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
	var out []string
	for p := range strings.SplitSeq(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
