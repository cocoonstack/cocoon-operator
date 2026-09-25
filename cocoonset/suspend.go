package cocoonset

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/podpatch"
	"github.com/cocoonstack/cocoon-operator/snapshot"
)

const (
	annotationSuspendingSince  = "cocoonset.cocoonstack.io/suspending-since"
	annotationHibernateReclaim = "cocoonset.cocoonstack.io/hibernate-reclaim"

	suspendTimeout = 3 * time.Minute
)

type hibernateReclaim struct {
	Generation int64    `json:"generation"`
	VMs        []string `json:"vms"`
}

// reconcileSuspend polls the registry and stays Suspending until every managed VM's snapshot lands.
func (r *Reconciler) reconcileSuspend(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (ctrl.Result, error) {
	if classified.main == nil {
		// reconcileSuspend only runs under Spec.Suspend, so restore intent is unconditional.
		return r.createMainAgent(ctx, cs, func() (map[string]struct{}, error) {
			return map[string]struct{}{agentPodName(cs.Name, 0): {}}, nil
		})
	}
	if err := r.applySuspend(ctx, classified); err != nil {
		return ctrl.Result{}, err
	}
	allHibernated, err := r.allOwnedPodsHibernated(ctx, cs, classified)
	if err != nil || !allHibernated {
		return r.pollSuspend(ctx, cs, classified, err)
	}
	if err := r.clearSuspendDeadline(ctx, cs); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseSuspended))
}

func (r *Reconciler) pollSuspend(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, probeErr error) (ctrl.Result, error) {
	exceeded, err := r.suspendDeadlineExceeded(ctx, cs)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !exceeded {
		if err := r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseSuspending)); err != nil {
			return ctrl.Result{}, err
		}
		if probeErr != nil {
			return ctrl.Result{}, probeErr
		}
		return ctrl.Result{RequeueAfter: requeueSuspendPoll}, nil
	}
	msg := fmt.Sprintf("not every managed VM was hibernated with its snapshot in the registry within %s", suspendTimeout)
	if probeErr != nil {
		msg += ": " + probeErr.Error()
	}
	commonk8s.Eventf(r.Recorder, cs, corev1.EventTypeWarning, "SuspendTimedOut", "%s", msg)
	return ctrl.Result{RequeueAfter: requeueSuspendPoll}, r.patchStatus(ctx, cs, buildStatus(cs, classified, cocoonv1.CocoonSetPhaseFailed))
}

func (r *Reconciler) suspendDeadlineExceeded(ctx context.Context, cs *cocoonv1.CocoonSet) (bool, error) {
	since, err := time.Parse(time.RFC3339, cs.Annotations[annotationSuspendingSince])
	if cs.Status.Phase != cocoonv1.CocoonSetPhaseSuspending || err != nil {
		return false, r.patchAnnotation(ctx, cs, annotationSuspendingSince, time.Now().UTC().Format(time.RFC3339))
	}
	return time.Since(since) > suspendTimeout, nil
}

func (r *Reconciler) clearSuspendDeadline(ctx context.Context, cs *cocoonv1.CocoonSet) error {
	if _, ok := cs.Annotations[annotationSuspendingSince]; !ok {
		return nil
	}
	return r.patchAnnotation(ctx, cs, annotationSuspendingSince, "")
}

// allOwnedPodsHibernated returns (false, nil), not an error, while the expected state is not yet observed.
func (r *Reconciler) allOwnedPodsHibernated(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, error) {
	for _, name := range slices.Sorted(maps.Keys(classified.allByName)) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		pod := classified.allByName[name]
		spec := meta.ParseVMSpec(pod)
		if !spec.Managed {
			continue
		}
		// a kubelet-terminal pod has no VM to snapshot; waiting on it would park the set in Suspending forever
		if meta.IsPodTerminal(pod) {
			continue
		}
		if spec.VMName == "" {
			return false, nil
		}
		// vk flips hibernated with observed-generation only after this round's push; a stale tag or lagging informer cannot pass
		if st := meta.ReadLifecycleStatus(pod); st.State != meta.LifecycleStateHibernated ||
			st.ObservedGeneration < cs.Generation {
			return false, nil
		}
		present, err := snapshot.HasHibernateSnapshot(ctx, r.Registry, spec.VMName)
		if err != nil {
			return false, err
		}
		if !present {
			return false, nil
		}
	}
	return true, nil
}

func (r *Reconciler) applySuspend(ctx context.Context, classified classifiedPods) error {
	return classified.forEachSorted(ctx, func(pod *corev1.Pod) error {
		if err := podpatch.HibernateState(ctx, r.Client, pod, true); err != nil {
			return fmt.Errorf("patch hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		return nil
	})
}

// applyUnsuspend skips pods targeted by an active CocoonHibernation CR to avoid racing that reconciler.
func (r *Reconciler) applyUnsuspend(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) error {
	var hibernated []*corev1.Pod
	for _, name := range slices.Sorted(maps.Keys(classified.allByName)) {
		if pod := classified.allByName[name]; meta.ReadHibernateState(pod) {
			hibernated = append(hibernated, pod)
		}
	}
	if len(hibernated) == 0 {
		return nil
	}

	hibernatedByCR, err := r.podsHibernatedByCR(ctx, cs.Namespace)
	if err != nil {
		return err
	}
	hibernated = slices.DeleteFunc(hibernated, func(pod *corev1.Pod) bool {
		_, ownedByCR := hibernatedByCR[pod.Name]
		return ownedByCR
	})
	if len(hibernated) == 0 {
		return nil
	}
	owed := readHibernateReclaim(cs)
	owed.Generation = cs.Generation
	for _, pod := range hibernated {
		if spec := meta.ParseVMSpec(pod); spec.Managed && spec.VMName != "" && !slices.Contains(owed.VMs, spec.VMName) {
			owed.VMs = append(owed.VMs, spec.VMName)
		}
	}
	if err := r.writeHibernateReclaim(ctx, cs, owed); err != nil {
		return err
	}
	for _, pod := range hibernated {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err := podpatch.HibernateState(ctx, r.Client, pod, false); err != nil {
			return fmt.Errorf("clear hibernate annotation on %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

func (r *Reconciler) reclaimWokenSnapshots(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) error {
	owed := readHibernateReclaim(cs)
	if len(owed.VMs) == 0 {
		return nil
	}
	woken := map[string]bool{}
	for _, pod := range classified.allByName {
		if wokenAt(pod, owed.Generation) {
			woken[meta.ParseVMSpec(pod).VMName] = true
		}
	}
	var pending []string
	var errs []error
	for _, vm := range owed.VMs {
		if woken[vm] {
			err := snapshot.DeleteManifestIfPresent(ctx, r.Registry, vm, meta.HibernateSnapshotTag)
			if err == nil {
				continue
			}
			errs = append(errs, fmt.Errorf("reclaim %s:%s: %w", vm, meta.HibernateSnapshotTag, err))
		}
		pending = append(pending, vm)
	}
	if len(pending) < len(owed.VMs) {
		owed.VMs = pending
		errs = append(errs, r.writeHibernateReclaim(ctx, cs, owed))
	}
	return errors.Join(errs...)
}

func (r *Reconciler) writeHibernateReclaim(ctx context.Context, cs *cocoonv1.CocoonSet, owed hibernateReclaim) error {
	value := ""
	if len(owed.VMs) > 0 {
		raw, err := json.Marshal(owed)
		if err != nil {
			return fmt.Errorf("encode hibernate reclaim of %s/%s: %w", cs.Namespace, cs.Name, err)
		}
		value = string(raw)
	}
	if cs.Annotations[annotationHibernateReclaim] == value {
		return nil
	}
	return r.patchAnnotation(ctx, cs, annotationHibernateReclaim, value)
}

func (r *Reconciler) podsHibernatedByCR(ctx context.Context, namespace string) (map[string]struct{}, error) {
	return r.hibernationPodNames(ctx, namespace, func(h *cocoonv1.CocoonHibernation) bool {
		return h.Spec.Desire == cocoonv1.HibernationDesireHibernate || h.Status.Phase == cocoonv1.CocoonHibernationPhaseHibernating
	})
}

func readHibernateReclaim(cs *cocoonv1.CocoonSet) hibernateReclaim {
	var owed hibernateReclaim
	if raw := cs.Annotations[annotationHibernateReclaim]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &owed); err != nil {
			return hibernateReclaim{}
		}
	}
	return owed
}

func wokenAt(pod *corev1.Pod, generation int64) bool {
	st := meta.ReadLifecycleStatus(pod)
	return !bool(meta.ReadHibernateState(pod)) && meta.VMLive(pod) &&
		st.State == meta.LifecycleStateReady && st.ObservedGeneration >= generation
}
