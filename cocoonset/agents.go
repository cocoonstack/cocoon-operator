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

// subAgentCreateConcurrency caps parallel creates so a scale-up does not burst the apiserver.
const subAgentCreateConcurrency = 8

// ensureSubAgents reports changed on any mutation and the shortest rebuild backoff still pending.
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
		changed = changed || deleted
		if wait > 0 && (requeueAfter == 0 || wait < requeueAfter) {
			requeueAfter = wait
		}
	}

	created, err := r.createSubAgents(ctx, logger, cs, missing, mainVMName, mainNodeName, intent)
	changed = changed || created
	if err != nil {
		return changed, requeueAfter, err
	}

	for _, slot := range slices.Sorted(maps.Keys(classified.sub)) {
		if slot <= cs.Spec.Agent.Replicas {
			continue
		}
		pod := classified.sub[slot]
		if err := r.stashDeleteVMNames(ctx, cs, []corev1.Pod{*pod}); err != nil {
			return changed, requeueAfter, fmt.Errorf("stash vm name of sub-agent slot %d: %w", slot, err)
		}
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

func (r *Reconciler) createSubAgents(ctx context.Context, logger *log.Fields, cs *cocoonv1.CocoonSet, missing []int32, mainVMName, mainNodeName string, intent restoreIntent) (bool, error) {
	if len(missing) == 0 {
		return false, nil
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
			if err := r.markRestoreFromIntent(gctx, subPod, intent); err != nil {
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

// triageSubAgent returns a non-zero requeueAfter while the slot waits out rebuild backoff.
func (r *Reconciler) triageSubAgent(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, slot int32) (bool, time.Duration, error) {
	if pod.Annotations[annotationDeadLetter] == "true" {
		return r.rebuildDeadLetteredOnDrift(ctx, logger, pod, cs, slot)
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

// rebuildDeadLetteredOnDrift leaves a dead-lettered pod alone until a spec edit, which earns a fresh rebuild budget.
func (r *Reconciler) rebuildDeadLetteredOnDrift(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, slot int32) (bool, time.Duration, error) {
	if podSpecMatchesAgent(pod, cs, slot) {
		return false, 0, nil
	}
	history := readRebuildHistory(cs)
	if _, ok := history[slot]; ok {
		delete(history, slot)
		if err := r.patchRebuildHistory(ctx, cs, history); err != nil {
			return false, 0, fmt.Errorf("reset rebuild history for slot %d: %w", slot, err)
		}
	}
	logger.Infof(ctx, "dead-lettered sub-agent %s/%s slot %d spec drifted, rebuilding with a fresh budget", pod.Namespace, pod.Name, slot)
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return false, 0, fmt.Errorf("delete dead-lettered sub-agent slot %d: %w", slot, err)
	}
	return true, 0, nil
}

// rebuildSubAgent persists history before the delete so a failed delete cannot bypass the gate.
func (r *Reconciler) rebuildSubAgent(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, slot int32) (bool, time.Duration, error) {
	history := readRebuildHistory(cs)
	entry := history[slot]
	if entry.Count >= maxRebuildAttempts {
		if err := r.patchAnnotation(ctx, pod, annotationDeadLetter, "true"); err != nil {
			return false, 0, err
		}
		metrics.SubAgentDeadLetterTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
		commonk8s.Eventf(r.Recorder, cs, corev1.EventTypeWarning, "SubAgentDeadLetter",
			"slot %d exhausted %d rebuilds; pod %s left in dead-letter", slot, maxRebuildAttempts, pod.Name)
		return false, 0, nil
	}
	if wait := backoffDelay(entry.Count); wait > 0 {
		remaining := wait - time.Since(entry.LastDeleted)
		if remaining > 0 {
			return false, remaining, nil
		}
	}
	entry.Count++
	entry.LastDeleted = time.Now()
	history[slot] = entry
	if err := r.patchRebuildHistory(ctx, cs, history); err != nil {
		return false, 0, fmt.Errorf("persist rebuild history: %w", err)
	}
	logger.Infof(ctx, "sub-agent %s/%s slot %d terminal (phase=%s lifecycle=%s), rebuild attempt %d/%d",
		pod.Namespace, pod.Name, slot, pod.Status.Phase, meta.ReadLifecycleState(pod), entry.Count, maxRebuildAttempts)
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return false, 0, fmt.Errorf("delete terminal sub-agent slot %d: %w", slot, err)
	}
	metrics.SubAgentRebuildTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
	commonk8s.Eventf(r.Recorder, cs, corev1.EventTypeNormal, "SubAgentRebuilding",
		"slot %d attempt %d/%d", slot, entry.Count, maxRebuildAttempts)
	return true, 0, nil
}

// patchAnnotation merge-patches one annotation on obj; an empty value deletes the key.
func (r *Reconciler) patchAnnotation(ctx context.Context, obj client.Object, key, value string) error {
	var v any = value
	if value == "" {
		v = nil
	}
	patch, err := commonk8s.AnnotationsMergePatch(map[string]any{key: v})
	if err != nil {
		return fmt.Errorf("build patch for %T %s/%s annotation %s: %w", obj, obj.GetNamespace(), obj.GetName(), key, err)
	}
	if err := r.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("patch %T %s/%s annotation %s: %w", obj, obj.GetNamespace(), obj.GetName(), key, err)
	}
	return nil
}

// patchRebuildHistory mirrors the annotation onto cs so later slots in this reconcile see fresh history.
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
