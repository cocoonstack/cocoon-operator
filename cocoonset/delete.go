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

	// All pods gone — safe to GC snapshot tags from epoch. Walk both the
	// :latest tag (snapshotPolicy=always pushes) and the :hibernate tag
	// (CocoonHibernation pushes regardless of policy); leaving the latter
	// behind would let a same-named CocoonSet recreated later wake into
	// the previous generation's guest memory.
	if r.Epoch != nil {
		for i := range owned {
			spec := meta.ParseVMSpec(&owned[i])
			if spec.VMName == "" {
				continue
			}
			for _, tag := range []string{meta.DefaultSnapshotTag, meta.HibernateSnapshotTag} {
				if err := r.Epoch.DeleteManifest(ctx, spec.VMName, tag); err != nil {
					logger.Warnf(ctx, "delete snapshot %s:%s: %v", spec.VMName, tag, err)
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
