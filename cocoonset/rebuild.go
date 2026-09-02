package cocoonset

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
	"github.com/cocoonstack/cocoon-operator/metrics"
)

const (
	annotationRebuildHistory = "cocoonset.cocoonstack.io/rebuild-history"
	annotationDeadLetter     = "cocoonset.cocoonstack.io/dead-letter"

	maxRebuildAttempts = 4
)

// rebuildEntry persists in a CocoonSet annotation, keyed by pod name, so the count survives the pod delete.
type rebuildEntry struct {
	Count       int       `json:"count"`
	LastDeleted time.Time `json:"lastDeleted"`
}

// triagePod deletes a terminal or drifted pod for recreate within its rebuild budget; a dead-lettered pod waits for a spec edit.
func (r *Reconciler) triagePod(ctx context.Context, logger *log.Fields, cs *cocoonv1.CocoonSet, pod *corev1.Pod, matches bool) (bool, time.Duration, error) {
	var reason string
	switch {
	case podIsTerminal(pod):
		reason = fmt.Sprintf("terminal (phase=%s lifecycle=%s)", pod.Status.Phase, meta.ReadLifecycleState(pod))
	case !matches:
		reason = "spec drifted"
	default:
		return false, 0, nil
	}
	parkedAt, parked := pod.Annotations[annotationDeadLetter]
	if !parked {
		return r.rebuildPod(ctx, logger, cs, pod, reason)
	}
	if parkedAt == strconv.FormatInt(cs.Generation, 10) {
		return false, 0, nil
	}
	history := readRebuildHistory(cs)
	if _, ok := history[pod.Name]; ok {
		delete(history, pod.Name)
		if err := r.patchRebuildHistory(ctx, cs, history); err != nil {
			return false, 0, fmt.Errorf("reset rebuild history for %s: %w", pod.Name, err)
		}
	}
	logger.Infof(ctx, "dead-lettered pod %s/%s %s after a spec edit, rebuilding with a fresh budget", pod.Namespace, pod.Name, reason)
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return false, 0, fmt.Errorf("delete dead-lettered %s: %w", pod.Name, err)
	}
	return true, 0, nil
}

// rebuildPod persists history before the delete so a failed delete cannot bypass the gate; past the budget it dead-letters the pod at the current generation.
func (r *Reconciler) rebuildPod(ctx context.Context, logger *log.Fields, cs *cocoonv1.CocoonSet, pod *corev1.Pod, reason string) (bool, time.Duration, error) {
	history := readRebuildHistory(cs)
	entry := history[pod.Name]
	if entry.Count >= maxRebuildAttempts {
		if err := r.patchAnnotation(ctx, pod, annotationDeadLetter, strconv.FormatInt(cs.Generation, 10)); err != nil {
			return false, 0, err
		}
		metrics.SubAgentDeadLetterTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
		commonk8s.Eventf(r.Recorder, cs, corev1.EventTypeWarning, "SubAgentDeadLetter",
			"pod %s exhausted %d rebuilds (%s); left in dead-letter", pod.Name, maxRebuildAttempts, reason)
		return false, 0, nil
	}
	if wait := backoffDelay(entry.Count); wait > 0 {
		if remaining := wait - time.Since(entry.LastDeleted); remaining > 0 {
			return false, remaining, nil
		}
	}
	entry.Count++
	entry.LastDeleted = time.Now()
	history[pod.Name] = entry
	if err := r.patchRebuildHistory(ctx, cs, history); err != nil {
		return false, 0, fmt.Errorf("persist rebuild history: %w", err)
	}
	logger.Infof(ctx, "pod %s/%s %s, rebuild attempt %d/%d", pod.Namespace, pod.Name, reason, entry.Count, maxRebuildAttempts)
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return false, 0, fmt.Errorf("delete %s for rebuild: %w", pod.Name, err)
	}
	metrics.SubAgentRebuildTotal.WithLabelValues(cs.Namespace, cs.Name).Inc()
	commonk8s.Eventf(r.Recorder, cs, corev1.EventTypeNormal, "SubAgentRebuilding",
		"pod %s attempt %d/%d (%s)", pod.Name, entry.Count, maxRebuildAttempts, reason)
	return true, 0, nil
}

// patchRebuildHistory mirrors the annotation onto cs so later pods in this reconcile see fresh history.
func (r *Reconciler) patchRebuildHistory(ctx context.Context, cs *cocoonv1.CocoonSet, history map[string]rebuildEntry) error {
	enc, err := encodeRebuildHistory(cs, history)
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

func readRebuildHistory(cs *cocoonv1.CocoonSet) map[string]rebuildEntry {
	m := map[string]rebuildEntry{}
	if raw := cs.Annotations[annotationRebuildHistory]; raw != "" {
		// json "null" leaves m nil; callers write to it
		if err := json.Unmarshal([]byte(raw), &m); err != nil || m == nil {
			return map[string]rebuildEntry{}
		}
	}
	return m
}

// encodeRebuildHistory keeps only the pods the spec still names.
func encodeRebuildHistory(cs *cocoonv1.CocoonSet, m map[string]rebuildEntry) (string, error) {
	keep := desiredPodNames(cs)
	kept := maps.Clone(m)
	maps.DeleteFunc(kept, func(name string, _ rebuildEntry) bool { return !keep[name] })
	raw, err := json.Marshal(kept)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func desiredPodNames(cs *cocoonv1.CocoonSet) map[string]bool {
	names := map[string]bool{}
	for slot := range cs.Spec.Agent.Replicas + 1 {
		names[agentPodName(cs.Name, slot)] = true
	}
	for _, tb := range cs.Spec.Toolboxes {
		names[toolboxPodName(cs.Name, tb.Name)] = true
	}
	return names
}

// backoffDelay returns the wait before the next rebuild attempt: 0, 1s, 5s, 30s.
func backoffDelay(priorCount int) time.Duration {
	switch priorCount {
	case 0:
		return 0
	case 1:
		return 1 * time.Second
	case 2:
		return 5 * time.Second
	default:
		return 30 * time.Second
	}
}
