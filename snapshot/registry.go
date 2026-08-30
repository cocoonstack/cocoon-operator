// Package snapshot holds the registry interface shared by the CocoonSet and CocoonHibernation reconcilers.
package snapshot

import (
	"context"
	"fmt"

	"github.com/cocoonstack/cocoon-common/meta"
)

// Registry is the subset of registry operations the reconcilers need; oci.OCIRegistry satisfies it.
type Registry interface {
	HasManifest(ctx context.Context, name, reference string) (bool, error)
	DeleteManifest(ctx context.Context, name, reference string) error
}

// DeleteManifestIfPresent probes first: some registries materialize an empty repository while authorizing a DELETE for a missing tag.
func DeleteManifestIfPresent(ctx context.Context, reg Registry, name, reference string) error {
	present, err := reg.HasManifest(ctx, name, reference)
	if err != nil {
		return fmt.Errorf("probe snapshot %s:%s: %w", name, reference, err)
	}
	if !present {
		return nil
	}
	return reg.DeleteManifest(ctx, name, reference)
}

// HasHibernateSnapshot performs the same :hibernate lookup vk-cocoon performs at wake.
func HasHibernateSnapshot(ctx context.Context, reg Registry, vmName string) (bool, error) {
	present, err := reg.HasManifest(ctx, vmName, meta.HibernateSnapshotTag)
	if err != nil {
		return false, fmt.Errorf("probe hibernate snapshot %s: %w", vmName, err)
	}
	return present, nil
}
