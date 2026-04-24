package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
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
// Returns (deleted, err). A non-deleted return means the pod still matches.
func (r *Reconciler) triageSubAgent(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, slot int32) (bool, error) {
	switch {
	case meta.IsPodTerminal(pod):
		logger.Infof(ctx, "sub-agent %s/%s slot %d terminal (phase=%s), deleting for recreate", pod.Namespace, pod.Name, slot, pod.Status.Phase)
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete terminal sub-agent slot %d: %w", slot, err)
		}
		return true, nil
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
