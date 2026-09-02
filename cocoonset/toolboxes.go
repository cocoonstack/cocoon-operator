package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// ensureToolboxes reports changed on any mutation and the shortest rebuild backoff still pending.
func (r *Reconciler) ensureToolboxes(ctx context.Context, cs *cocoonv1.CocoonSet, classified classifiedPods, intent restoreIntent) (bool, time.Duration, error) {
	logger := log.WithFunc("cocoonset.Reconciler.ensureToolboxes")
	// Webhook already rejects duplicates; validating before any Create/Delete keeps a bypass from leaving partial state.
	desired := make(map[string]bool, len(cs.Spec.Toolboxes))
	for _, tb := range cs.Spec.Toolboxes {
		if desired[tb.Name] {
			return false, 0, fmt.Errorf("duplicate toolbox name %q in spec", tb.Name)
		}
		if _, convErr := strconv.Atoi(tb.Name); convErr == nil {
			return false, 0, fmt.Errorf("toolbox name %q must not be an integer: collides with agent slot pod naming", tb.Name)
		}
		desired[tb.Name] = true
	}
	changed := false
	var requeueAfter time.Duration
	for _, tb := range cs.Spec.Toolboxes {
		podName := toolboxPodName(cs.Name, tb.Name)
		if classified.allByName[podName] != nil && classified.toolbox[tb.Name] == nil {
			return changed, requeueAfter, fmt.Errorf("create toolbox %s: name collision with existing pod %s", tb.Name, podName)
		}
		if pod, exists := classified.toolbox[tb.Name]; exists {
			deleted, wait, err := r.triagePod(ctx, logger, cs, pod, podSpecMatchesToolbox(pod, cs, tb))
			if err != nil {
				return changed, requeueAfter, err
			}
			changed = changed || deleted
			if wait > 0 && (requeueAfter == 0 || wait < requeueAfter) {
				requeueAfter = wait
			}
			continue
		}
		tbPod, err := buildToolboxPod(cs, tb, r.Scheme)
		if err != nil {
			return changed, requeueAfter, fmt.Errorf("build toolbox %s: %w", tb.Name, err)
		}
		if err := r.markRestoreFromIntent(ctx, tbPod, intent); err != nil {
			return changed, requeueAfter, fmt.Errorf("mark restore toolbox %s: %w", tb.Name, err)
		}
		if err := r.Create(ctx, tbPod); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return changed, requeueAfter, fmt.Errorf("create toolbox %s: %w", tb.Name, err)
			}
			if collisionErr := r.checkToolboxCollision(ctx, cs, tbPod, tb.Name); collisionErr != nil {
				return changed, requeueAfter, collisionErr
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
			return changed, requeueAfter, fmt.Errorf("stash vm name of toolbox %s: %w", name, err)
		}
		if err := r.Delete(ctx, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return changed, requeueAfter, fmt.Errorf("delete extra toolbox %s: %w", name, err)
		}
		logger.Infof(ctx, "deleted extra toolbox %s/%s", pod.Namespace, pod.Name)
		changed = true
	}
	return changed, requeueAfter, nil
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
