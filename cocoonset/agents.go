package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/metrics"
)

// subAgentCreateConcurrency caps parallel pod creates during fan-out so a
// large scale-up (e.g. 1→N) does not burst the apiserver. Empirically the
// rate limiter in controller-runtime plus apiserver QPS accommodate 8 in
// flight without priority-fairness throttling.
const subAgentCreateConcurrency = 8

// ensureSubAgents creates/deletes sub-agent pods to match [1..Replicas].
// Returns changed (true when cluster state was mutated) and requeueAfter
// (non-zero when a sub-agent is in rebuild backoff and the caller should
// re-reconcile when backoff elapses). Missing slots are created concurrently
// so batch scale-ups do not serialize N apiserver round trips.
func (r *Reconciler) ensureSubAgents(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, mainVMName, mainNodeName string, intent restoreIntent) (bool, time.Duration, error) {
	logger := log.WithFunc("cocoonset.Reconciler.ensureSubAgents")
	changed := false
	var requeueAfter time.Duration

	var missing []int32
	for slot := int32(1); slot <= cs.Spec.Agent.Replicas; slot++ {
		pod, exists := classified.sub[slot]
		if !exists {
			missing = append(missing, slot)
			continue
		}
		deleted, wait, err := r.triageSubAgent(ctx, logger, pod, cs, slot)
		if err != nil {
			return changed, requeueAfter, err
		}
		if deleted {
			changed = true
		}
		if wait > 0 && (requeueAfter == 0 || wait < requeueAfter) {
			requeueAfter = wait
		}
	}

	created, err := r.createSubAgents(ctx, logger, cs, missing, mainVMName, mainNodeName, intent)
	if created {
		changed = true
	}
	if err != nil {
		return changed, requeueAfter, err
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
			return changed, requeueAfter, fmt.Errorf("delete extra sub-agent slot %d: %w", slot, err)
		}
		logger.Infof(ctx, "deleted extra sub-agent %s/%s", pod.Namespace, pod.Name)
		changed = true
	}
	return changed, requeueAfter, nil
}

// createSubAgents builds the missing slots concurrently so a batch scale-up does
// not serialize N apiserver round trips.
func (r *Reconciler) createSubAgents(ctx context.Context, logger *log.Fields, cs *cocoonv1.CocoonSet, missing []int32, mainVMName, mainNodeName string, intent restoreIntent) (bool, error) {
	if len(missing) == 0 {
		return false, nil
	}
	// Resolved before the fan-out so the goroutines only ever read it.
	restorable, err := intent()
	if err != nil {
		return false, err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(subAgentCreateConcurrency)
	var created atomic.Bool
	for _, slot := range missing {
		g.Go(func() error {
			subPod, err := buildAgentPod(cs, slot, mainVMName, mainNodeName, r.Scheme)
			if err != nil {
				return fmt.Errorf("build sub-agent slot %d: %w", slot, err)
			}
			_, wantRestore := restorable[subPod.Name]
			if err := r.markRestoreIfHibernated(gctx, subPod, wantRestore); err != nil {
				return fmt.Errorf("mark restore sub-agent slot %d: %w", slot, err)
			}
			if err := r.Create(gctx, subPod); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return nil
				}
				return fmt.Errorf("create sub-agent slot %d: %w", slot, err)
			}
			logger.Infof(gctx, "created sub-agent %s/%s", subPod.Namespace, subPod.Name)
			created.Store(true)
			return nil
		})
	}
	waitErr := g.Wait()
	return created.Load(), waitErr
}

// triageSubAgent deletes pod when it is terminal or has drifted from spec.
// requeueAfter > 0 means the slot is in rebuild backoff and the caller should
// re-reconcile when the window elapses; deleted=false with requeueAfter=0
// means the pod still matches or is in dead-letter.
func (r *Reconciler) triageSubAgent(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, slot int32) (bool, time.Duration, error) {
	if pod.Annotations[annotationDeadLetter] == "true" {
		return false, 0, nil
	}
	switch {
	case podIsTerminal(pod):
		return r.rebuildSubAgent(ctx, logger, pod, cs, slot)
	case !podSpecMatchesAgent(pod, cs, slot):
		logger.Infof(ctx, "sub-agent %s/%s slot %d spec drifted, deleting for recreate", pod.Namespace, pod.Name, slot)
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return false, 0, fmt.Errorf("delete drifted sub-agent slot %d: %w", slot, err)
		}
		return true, 0, nil
	default:
		return false, 0, nil
	}
}

// rebuildSubAgent deletes pod with exponential backoff, dead-lettering past
// maxRebuildAttempts so failures stay visible. History persists before the
// delete so a failed delete cannot bypass the gate; the patch DeepCopies so
// concurrent ensureSubAgents goroutines observe a stable cs pointer.
func (r *Reconciler) rebuildSubAgent(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, slot int32) (bool, time.Duration, error) {
	history := readRebuildHistory(cs)
	entry := history[slot]
	if entry.Count >= maxRebuildAttempts {
		if err := r.patchPodAnnotation(ctx, pod, annotationDeadLetter, "true"); err != nil {
			return false, 0, err
		}
		metrics.SubAgentDeadLetterTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
		if r.Recorder != nil {
			r.Recorder.Eventf(cs, corev1.EventTypeWarning, "SubAgentDeadLetter",
				"slot %d exhausted %d rebuilds; pod %s left in dead-letter", slot, maxRebuildAttempts, pod.Name)
		}
		return false, 0, nil
	}
	if wait := backoffDelay(entry.Count); wait > 0 {
		remaining := wait - time.Since(entry.LastDeleted)
		if remaining > 0 {
			return false, remaining, nil
		}
	}
	next := maps.Clone(history)
	next[slot] = rebuildEntry{Count: entry.Count + 1, LastDeleted: time.Now()}
	if err := r.patchRebuildHistory(ctx, cs, next); err != nil {
		return false, 0, fmt.Errorf("persist rebuild history: %w", err)
	}
	logger.Infof(ctx, "sub-agent %s/%s slot %d terminal (phase=%s lifecycle=%s), rebuild attempt %d/%d",
		pod.Namespace, pod.Name, slot, pod.Status.Phase, meta.ReadLifecycleState(pod), next[slot].Count, maxRebuildAttempts)
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return false, 0, fmt.Errorf("delete terminal sub-agent slot %d: %w", slot, err)
	}
	metrics.SubAgentRebuildTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
	if r.Recorder != nil {
		r.Recorder.Eventf(cs, corev1.EventTypeNormal, "SubAgentRebuilding",
			"slot %d attempt %d/%d", slot, next[slot].Count, maxRebuildAttempts)
	}
	return true, 0, nil
}

func (r *Reconciler) patchPodAnnotation(ctx context.Context, pod *corev1.Pod, key, value string) error {
	patch, err := commonk8s.AnnotationsMergePatch(map[string]any{key: value})
	if err != nil {
		return fmt.Errorf("build patch for pod %s/%s annotation %s: %w", pod.Namespace, pod.Name, key, err)
	}
	if err := r.Patch(ctx, pod, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("patch pod %s/%s annotation %s: %w", pod.Namespace, pod.Name, key, err)
	}
	return nil
}

// patchRebuildHistory writes the new history into the CocoonSet annotation
// via Patch on a DeepCopy (Spec is what concurrent create goroutines read,
// not Annotations, so a local write to Annotations after the patch is safe
// and lets subsequent slot iterations see fresh history within the same
// reconcile).
func (r *Reconciler) patchRebuildHistory(ctx context.Context, cs *cocoonv1.CocoonSet, history map[int32]rebuildEntry) error {
	enc, err := encodeRebuildHistory(cs.Spec.Agent.Replicas, history)
	if err != nil {
		return fmt.Errorf("encode rebuild history: %w", err)
	}
	csCopy := cs.DeepCopy()
	if csCopy.Annotations == nil {
		csCopy.Annotations = map[string]string{}
	}
	csCopy.Annotations[annotationRebuildHistory] = enc
	if err := r.Patch(ctx, csCopy, client.MergeFrom(cs)); err != nil {
		return fmt.Errorf("patch rebuild history: %w", err)
	}
	if cs.Annotations == nil {
		cs.Annotations = map[string]string{}
	}
	cs.Annotations[annotationRebuildHistory] = enc
	return nil
}
