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

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
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
		deleted, wait, err := r.triagePod(ctx, logger, cs, pod, podSpecMatchesAgent(pod, cs, slot))
		if err != nil {
			return changed, requeueAfter, err
		}
		changed = changed || deleted
		if wait > 0 && (requeueAfter == 0 || wait < requeueAfter) {
			requeueAfter = wait
		}
	}

	missing = slices.DeleteFunc(missing, func(slot int32) bool { return budgetExhausted(cs, agentPodName(cs.Name, slot)) })
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
