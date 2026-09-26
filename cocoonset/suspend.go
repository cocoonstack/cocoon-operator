package cocoonset

import (
	"context"
	"encoding/json/v2"
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
	Restore    []string `json:"restore,omitempty"`
	Suspended  []string `json:"suspended,omitempty"`
}

// reconcileSuspend polls the registry and stays Suspending until every managed VM's snapshot lands.
func (r *Reconciler) reconcileSuspend(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (ctrl.Result, error) {
	if classified.main == nil {
		// reconcileSuspend only runs under Spec.Suspend, so restore intent is unconditional.
		return r.createMainAgent(ctx, cs, func() (map[string]struct{}, error) {
			return map[string]struct{}{agentPodName(cs.Name, 0): {}}, nil
		})
	}
	if err := r.applySuspend(ctx, cs, classified); err != nil {
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
		// A kubelet-terminal pod has no VM to snapshot, so waiting on it would park the set in Suspending forever.
		if meta.IsPodTerminal(pod) {
			continue
		}
		if spec.VMName == "" {
			return false, nil
		}
		// vk reports hibernated at this generation only after this round's push, so a stale tag cannot pass.
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

func (r *Reconciler) applySuspend(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) error {
	owed := readHibernateReclaim(cs)
	for _, name := range slices.Sorted(maps.Keys(classified.allByName)) {
		owed.VMs = appendManagedVM(owed.VMs, classified.allByName[name])
		owed.Suspended = appendManagedVM(owed.Suspended, classified.allByName[name])
	}
	if err := r.writeHibernateReclaim(ctx, cs, owed); err != nil {
		return err
	}
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
	if len(hibernated) > 0 {
		hibernatedByCR, err := r.podsHibernatedByCR(ctx, cs.Namespace)
		if err != nil {
			return err
		}
		hibernated = slices.DeleteFunc(hibernated, func(pod *corev1.Pod) bool {
			_, ownedByCR := hibernatedByCR[pod.Name]
			return ownedByCR
		})
	}
	owed := readHibernateReclaim(cs)
	if len(hibernated) == 0 && len(owed.Suspended) == 0 {
		return nil
	}
	owed.Generation = cs.Generation
	for _, pod := range hibernated {
		owed.VMs = appendManagedVM(owed.VMs, pod)
	}
	owed.Restore = appendMissingVMs(owed.Restore, owed.Suspended, classified)
	if len(hibernated) == 0 {
		owed.Suspended = nil
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
	owed.Suspended = nil
	return r.writeHibernateReclaim(ctx, cs, owed)
}

func (r *Reconciler) reclaimWokenSnapshots(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) error {
	owed := readHibernateReclaim(cs)
	if len(owed.VMs) == 0 {
		return nil
	}
	var woken []string
	for _, pod := range classified.allByName {
		if wokenAt(pod, owed.Generation) {
			woken = append(woken, meta.ParseVMSpec(pod).VMName)
		}
	}
	return r.reclaimSnapshots(ctx, cs, woken)
}

func (r *Reconciler) reclaimSnapshots(ctx context.Context, cs *cocoonv1.CocoonSet, vms []string) error {
	var reclaimed []string
	var errs []error
	for _, vm := range readHibernateReclaim(cs).VMs {
		if !slices.Contains(vms, vm) {
			continue
		}
		if err := snapshot.DeleteManifestIfPresent(ctx, r.Registry, vm, meta.HibernateSnapshotTag); err != nil {
			errs = append(errs, fmt.Errorf("reclaim %s:%s: %w", vm, meta.HibernateSnapshotTag, err))
			continue
		}
		reclaimed = append(reclaimed, vm)
	}
	return errors.Join(append(errs, r.forgetReclaim(ctx, cs, reclaimed))...)
}

func (r *Reconciler) forgetReclaim(ctx context.Context, cs *cocoonv1.CocoonSet, vms []string) error {
	owed := readHibernateReclaim(cs)
	forget := func(vm string) bool { return slices.Contains(vms, vm) }
	if !slices.ContainsFunc(owed.VMs, forget) {
		return nil
	}
	owed.VMs = slices.DeleteFunc(owed.VMs, forget)
	owed.Restore = slices.DeleteFunc(owed.Restore, forget)
	owed.Suspended = slices.DeleteFunc(owed.Suspended, forget)
	return r.writeHibernateReclaim(ctx, cs, owed)
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

func appendManagedVM(vms []string, pod *corev1.Pod) []string {
	if spec := meta.ParseVMSpec(pod); spec.Managed && spec.VMName != "" && !slices.Contains(vms, spec.VMName) {
		return append(vms, spec.VMName)
	}
	return vms
}

func appendMissingVMs(restore, vms []string, classified classifiedPods) []string {
	live := make(map[string]bool, len(classified.allByName))
	for _, pod := range classified.allByName {
		live[meta.ParseVMSpec(pod).VMName] = true
	}
	for _, vm := range vms {
		if !live[vm] && !slices.Contains(restore, vm) {
			restore = append(restore, vm)
		}
	}
	return restore
}

func wokenAt(pod *corev1.Pod, generation int64) bool {
	st := meta.ReadLifecycleStatus(pod)
	return !bool(meta.ReadHibernateState(pod)) && meta.VMLive(pod) &&
		st.State == meta.LifecycleStateReady && st.ObservedGeneration >= generation
}
