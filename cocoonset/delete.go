package cocoonset

import (
	"context"
	"fmt"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

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

	// Requeue if any pods still exist — vk-cocoon's DeletePod may still
	// be running snapshot save/push. We only GC epoch tags and remove the
	// finalizer once every pod is fully gone from the API server.
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

	// Pods are gone by the time we GC, so read VM names from Status, not from a re-list.
	// :hibernate is pushed regardless of snapshotPolicy, so drop both tags unconditionally.
	if r.Epoch != nil {
		for _, name := range vmNamesFromStatus(cs) {
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

// vmNamesFromStatus collects VM names for the agents and toolboxes the
// reconciler observed before delete; pods themselves are already gone.
func vmNamesFromStatus(cs *cocoonv1.CocoonSet) []string {
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
