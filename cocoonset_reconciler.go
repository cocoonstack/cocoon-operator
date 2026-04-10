package main

import (
	"context"
	"fmt"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cocoonv1alpha1 "github.com/cocoonstack/cocoon-common/apis/v1alpha1"
)

const (
	// finalizerName is added to every CocoonSet so the reconciler
	// gets a chance to delete owned pods (and optionally garbage
	// collect snapshots) before the API server actually removes the
	// object.
	finalizerName = "cocoonset.cocoonstack.io/finalizer"
)

// CocoonSetReconciler reconciles a CocoonSet object.
type CocoonSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Epoch  SnapshotRegistry
}

// SetupWithManager registers the reconciler against the supplied
// controller-runtime manager.
func (r *CocoonSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cocoonv1alpha1.CocoonSet{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

// Reconcile is the entry point invoked by controller-runtime each
// time a watched event lands. It is intentionally short — every
// non-trivial subroutine lives in cocoonset_pods.go or
// cocoonset_status.go so this file stays a high-level outline.
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
		// Re-queue: the next iteration will see a fresh copy.
		return ctrl.Result{Requeue: true}, nil
	}

	logger.Debugf(ctx, "reconcile cocoonset %s/%s gen=%d", cs.Namespace, cs.Name, cs.Generation)

	// Subsequent commits replace this with the full reconcile loop:
	// list owned pods, classify, ensure main, ensure sub-agents,
	// ensure toolboxes, build status, and patch /status.
	return ctrl.Result{}, nil
}

// reconcileDelete tears down everything the CocoonSet owns and then
// removes the finalizer so the API server can finalize the delete.
// The pod-deletion / snapshot GC body is filled in by a later commit.
func (r *CocoonSetReconciler) reconcileDelete(ctx context.Context, cs *cocoonv1alpha1.CocoonSet) (ctrl.Result, error) {
	logger := log.WithFunc("CocoonSetReconciler.reconcileDelete")
	logger.Infof(ctx, "deleting cocoonset %s/%s", cs.Namespace, cs.Name)

	// owned-pod cleanup + epoch.DeleteManifest live in a later commit.

	if controllerutil.ContainsFinalizer(cs, finalizerName) {
		controllerutil.RemoveFinalizer(cs, finalizerName)
		if err := r.Update(ctx, cs); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
	}
	return ctrl.Result{}, nil
}
