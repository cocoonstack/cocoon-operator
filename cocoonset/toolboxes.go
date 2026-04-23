package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// ensureToolboxes creates/deletes toolbox pods to match spec.
// Returns true when cluster state was mutated.
func (r *Reconciler) ensureToolboxes(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods) (bool, error) {
	logger := log.WithFunc("cocoonset.Reconciler.ensureToolboxes")
	changed := false
	desired := map[string]bool{}
	for _, tb := range cs.Spec.Toolboxes {
		desired[tb.Name] = true
		podName := fmt.Sprintf("%s-%s", cs.Name, tb.Name)
		if classified.allByName[podName] != nil && classified.toolbox[tb.Name] == nil {
			return changed, fmt.Errorf("create toolbox %s: name collision with existing pod %s", tb.Name, podName)
		}
		if pod, exists := classified.toolbox[tb.Name]; exists {
			deleted, err := r.triageToolbox(ctx, logger, pod, cs, tb)
			if err != nil {
				return changed, err
			}
			if deleted {
				changed = true
			}
			continue
		}
		tbPod := buildToolboxPod(cs, tb, r.Scheme)
		if err := r.Create(ctx, tbPod); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return changed, fmt.Errorf("create toolbox %s: %w", tb.Name, err)
			}
			if collisionErr := r.checkToolboxCollision(ctx, cs, tbPod, tb.Name); collisionErr != nil {
				return changed, collisionErr
			}
			continue
		}
		logger.Infof(ctx, "created toolbox %s/%s", tbPod.Namespace, tbPod.Name)
		changed = true
	}
	for _, name := range slices.Sorted(maps.Keys(classified.toolbox)) {
		if desired[name] {
			continue
		}
		pod := classified.toolbox[name]
		if err := r.Delete(ctx, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return changed, fmt.Errorf("delete extra toolbox %s: %w", name, err)
		}
		logger.Infof(ctx, "deleted extra toolbox %s/%s", pod.Namespace, pod.Name)
		changed = true
	}
	return changed, nil
}

// triageToolbox deletes pod when it is terminal or has drifted from spec.
// Returns (deleted, err). A non-deleted return means the pod still matches.
func (r *Reconciler) triageToolbox(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, tb cocoonv1.ToolboxSpec) (bool, error) {
	switch {
	case meta.IsPodTerminal(pod):
		logger.Infof(ctx, "toolbox %s/%s %q terminal (phase=%s), deleting for recreate", pod.Namespace, pod.Name, tb.Name, pod.Status.Phase)
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete terminal toolbox %s: %w", tb.Name, err)
		}
		return true, nil
	case !podSpecMatchesToolbox(pod, cs, tb):
		logger.Infof(ctx, "toolbox %s/%s %q spec drifted, deleting for recreate", pod.Namespace, pod.Name, tb.Name)
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete drifted toolbox %s: %w", tb.Name, err)
		}
		return true, nil
	default:
		return false, nil
	}
}

func (r *Reconciler) checkToolboxCollision(ctx context.Context, cs *cocoonv1.CocoonSet, tbPod *corev1.Pod, tbName string) error {
	var existing corev1.Pod
	if err := r.Get(ctx, client.ObjectKeyFromObject(tbPod), &existing); err != nil {
		return fmt.Errorf("get existing pod %s/%s: %w", tbPod.Namespace, tbPod.Name, err)
	}
	if existing.Labels[meta.LabelRole] == meta.RoleToolbox && metav1.IsControlledBy(&existing, cs) {
		return nil
	}
	return fmt.Errorf("create toolbox %s: name collision with existing pod %s/%s (role=%s)", tbName, tbPod.Namespace, tbPod.Name, existing.Labels[meta.LabelRole])
}
