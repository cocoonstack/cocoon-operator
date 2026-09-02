// Package podpatch holds the pod annotation patches the operator owns: each one short-circuits when the pod already carries the desired state.
package podpatch

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

// HibernateState sets the hibernate annotation to state.
func HibernateState(ctx context.Context, cli client.Client, pod *corev1.Pod, state bool) error {
	if meta.ReadHibernateState(pod) == meta.HibernateState(state) {
		return nil
	}
	return commonk8s.Patch(ctx, cli, pod, func(p *corev1.Pod) {
		meta.HibernateState(state).Apply(p)
	})
}

// KeepSnapshotOnDelete flags the pod's deletion as a seat release.
func KeepSnapshotOnDelete(ctx context.Context, cli client.Client, pod *corev1.Pod) error {
	if meta.ReadKeepSnapshotOnDelete(pod) {
		return nil
	}
	return commonk8s.Patch(ctx, cli, pod, meta.MarkKeepSnapshotOnDelete)
}

// CocoonSetGeneration stamps the owning CocoonSet's metadata.generation onto the pod.
func CocoonSetGeneration(ctx context.Context, cli client.Client, pod *corev1.Pod, generation int64) error {
	if meta.ReadCocoonSetGeneration(pod) == generation {
		return nil
	}
	return commonk8s.Patch(ctx, cli, pod, func(p *corev1.Pod) {
		meta.StampCocoonSetGeneration(p, generation)
	})
}
