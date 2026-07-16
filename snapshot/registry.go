// Package snapshot contains the shared snapshot-registry interface used by
// both the CocoonSet and CocoonHibernation reconcilers.
package snapshot

import (
	"context"
	"fmt"

	"github.com/cocoonstack/cocoon-common/meta"
)

// Registry is the subset of registry operations the operator reconcilers need.
// cocoon-common's oci.OCIRegistry satisfies it; tests swap in fakes.
type Registry interface {
	HasManifest(ctx context.Context, name, reference string) (bool, error)
	DeleteManifest(ctx context.Context, name, reference string) error
}

// HasHibernateSnapshot reports whether vmName has a :hibernate snapshot in the
// registry — the same lookup vk-cocoon performs at wake.
func HasHibernateSnapshot(ctx context.Context, reg Registry, vmName string) (bool, error) {
	present, err := reg.HasManifest(ctx, vmName, meta.HibernateSnapshotTag)
	if err != nil {
		return false, fmt.Errorf("probe hibernate snapshot %s: %w", vmName, err)
	}
	return present, nil
}
