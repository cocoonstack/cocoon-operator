package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// ensureSubAgents creates/deletes sub-agent pods to match [1..Replicas].
// Returns true when cluster state was mutated.
func (r *Reconciler) ensureSubAgents(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, mainVMName, mainNodeName string) (bool, error) {
	logger := log.WithFunc("cocoonset.Reconciler.ensureSubAgents")
	changed := false
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
		subPod := buildAgentPod(cs, slot, mainVMName, mainNodeName, r.Scheme)
		if err := r.Create(ctx, subPod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return changed, fmt.Errorf("create sub-agent slot %d: %w", slot, err)
		}
		logger.Infof(ctx, "created sub-agent %s/%s", subPod.Namespace, subPod.Name)
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
