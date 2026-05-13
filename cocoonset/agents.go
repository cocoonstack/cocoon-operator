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
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/metrics"
)

// subAgentCreateConcurrency caps parallel pod creates during fan-out so a
// large scale-up (e.g. 1→N) does not burst the apiserver. Empirically the
// rate limiter in controller-runtime plus apiserver QPS accommodate 8 in
// flight without priority-fairness throttling.
const subAgentCreateConcurrency = 8

// ensureSubAgents creates/deletes sub-agent pods to match [1..Replicas].
// Returns true when cluster state was mutated. Missing slots are created
// concurrently so batch scale-ups do not serialize N apiserver round trips.
func (r *Reconciler) ensureSubAgents(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, mainVMName, mainNodeName string) (bool, error) {
	logger := log.WithFunc("cocoonset.Reconciler.ensureSubAgents")
	changed := false

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(subAgentCreateConcurrency)
	var created atomic.Bool
	for slot := int32(1); slot <= cs.Spec.Agent.Replicas; slot++ {
		if pod, exists := classified.sub[slot]; exists {
			deleted, err := r.triageSubAgent(ctx, logger, pod, cs, slot)
			if err != nil {
				return changed, err
			}
			if deleted {
				changed = true
			}
			continue
		}
		g.Go(func() error {
			subPod, err := buildAgentPod(cs, slot, mainVMName, mainNodeName, r.Scheme)
			if err != nil {
				return fmt.Errorf("build sub-agent slot %d: %w", slot, err)
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
	if err := g.Wait(); err != nil {
		return changed || created.Load(), err
	}
	if created.Load() {
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

// triageSubAgent deletes pod when it is terminal or has drifted from spec.
// Returns (deleted, err). A non-deleted return means the pod still matches,
// is in dead-letter, or is waiting on the rebuild backoff.
func (r *Reconciler) triageSubAgent(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, slot int32) (bool, error) {
	if pod.Annotations[annotationDeadLetter] == "true" {
		return false, nil
	}
	switch {
	case meta.IsPodTerminal(pod):
		return r.rebuildSubAgent(ctx, logger, pod, cs, slot)
	case !podSpecMatchesAgent(pod, cs, slot):
		logger.Infof(ctx, "sub-agent %s/%s slot %d spec drifted, deleting for recreate", pod.Namespace, pod.Name, slot)
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete drifted sub-agent slot %d: %w", slot, err)
		}
		return true, nil
	default:
		return false, nil
	}
}

// rebuildSubAgent deletes pod with exponential backoff and a dead-letter
// gate. Past maxRebuildAttempts the pod is marked dead-letter and left in
// place so the failure stays visible and rebuild storms cannot consume
// the apiserver budget.
func (r *Reconciler) rebuildSubAgent(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, slot int32) (bool, error) {
	history := readRebuildHistory(cs)
	entry := history[slot]
	if entry.Count >= maxRebuildAttempts {
		if err := r.patchPodAnnotation(ctx, pod, annotationDeadLetter, "true"); err != nil {
			return false, err
		}
		metrics.SubAgentDeadLetterTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
		if r.Recorder != nil {
			r.Recorder.Eventf(cs, corev1.EventTypeWarning, "SubAgentDeadLetter",
				"slot %d exhausted %d rebuilds; pod %s left in dead-letter", slot, maxRebuildAttempts, pod.Name)
		}
		return false, nil
	}
	if wait := backoffDelay(entry.Count); wait > 0 && time.Since(entry.LastDeleted) < wait {
		return false, nil
	}
	logger.Infof(ctx, "sub-agent %s/%s slot %d terminal (phase=%s), rebuild attempt %d/%d",
		pod.Namespace, pod.Name, slot, pod.Status.Phase, entry.Count+1, maxRebuildAttempts)
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("delete terminal sub-agent slot %d: %w", slot, err)
	}
	entry.Count++
	entry.LastDeleted = time.Now()
	history[slot] = entry
	if err := writeRebuildHistory(cs, history); err != nil {
		return true, fmt.Errorf("encode rebuild history: %w", err)
	}
	if err := r.Update(ctx, cs); err != nil {
		return true, fmt.Errorf("persist rebuild history: %w", err)
	}
	metrics.SubAgentRebuildTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
	if r.Recorder != nil {
		r.Recorder.Eventf(cs, corev1.EventTypeNormal, "SubAgentRebuilding",
			"slot %d attempt %d/%d", slot, entry.Count, maxRebuildAttempts)
	}
	return true, nil
}

// patchPodAnnotation sets a single annotation via a strategic merge patch.
func (r *Reconciler) patchPodAnnotation(ctx context.Context, pod *corev1.Pod, key, value string) error {
	patch := fmt.Appendf(nil, `{"metadata":{"annotations":{%q:%q}}}`, key, value)
	if err := r.Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, patch)); err != nil {
		return fmt.Errorf("patch pod %s/%s annotation %s: %w", pod.Namespace, pod.Name, key, err)
	}
	return nil
}
