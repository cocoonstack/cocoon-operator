package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

func (r *Reconciler) ensureToolboxes(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, intent restoreIntent) (bool, error) {
	logger := log.WithFunc("cocoonset.Reconciler.ensureToolboxes")
	// Webhook already rejects duplicates; validating before any Create/Delete keeps a bypass from leaving partial state.
	desired := make(map[string]bool, len(cs.Spec.Toolboxes))
	for _, tb := range cs.Spec.Toolboxes {
		if desired[tb.Name] {
			return false, fmt.Errorf("duplicate toolbox name %q in spec", tb.Name)
		}
		if _, convErr := strconv.Atoi(tb.Name); convErr == nil {
			return false, fmt.Errorf("toolbox name %q must not be an integer: collides with agent slot pod naming", tb.Name)
		}
		desired[tb.Name] = true
	}
	changed := false
	for _, tb := range cs.Spec.Toolboxes {
		podName := toolboxPodName(cs.Name, tb.Name)
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
		tbPod, err := buildToolboxPod(cs, tb, r.Scheme)
		if err != nil {
			return changed, fmt.Errorf("build toolbox %s: %w", tb.Name, err)
		}
		restorable, err := intent()
		if err != nil {
			return changed, err
		}
		_, wantRestore := restorable[tbPod.Name]
		if err := r.markRestoreIfHibernated(ctx, tbPod, wantRestore); err != nil {
			return changed, fmt.Errorf("mark restore toolbox %s: %w", tb.Name, err)
		}
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
		if err := r.stashDeleteVMNames(ctx, cs, []corev1.Pod{*pod}); err != nil {
			return changed, fmt.Errorf("stash vm name of toolbox %s: %w", name, err)
		}
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

func (r *Reconciler) triageToolbox(ctx context.Context, logger *log.Fields, pod *corev1.Pod, cs *cocoonv1.CocoonSet, tb cocoonv1.ToolboxSpec) (bool, error) {
	var reason string
	switch {
	case podIsTerminal(pod):
		reason = fmt.Sprintf("terminal (phase=%s lifecycle=%s)", pod.Status.Phase, meta.ReadLifecycleState(pod))
	case !podSpecMatchesToolbox(pod, cs, tb):
		reason = "spec drifted"
	default:
		return false, nil
	}
	logger.Infof(ctx, "toolbox %s/%s %q %s, deleting for recreate", pod.Namespace, pod.Name, tb.Name, reason)
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("delete toolbox %s for recreate: %w", tb.Name, err)
	}
	return true, nil
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
