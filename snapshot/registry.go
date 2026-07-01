// Package snapshot contains the shared snapshot-registry interface used by
// both the CocoonSet and CocoonHibernation reconcilers.
package snapshot

import "context"

// Registry is the subset of registry operations the operator reconcilers need.
// Both cocoon-common's oci.OCIRegistry and epoch's registryclient.Client
// satisfy it; tests swap in fakes.
type Registry interface {
	HasManifest(ctx context.Context, name, reference string) (bool, error)
	DeleteManifest(ctx context.Context, name, reference string) error
}
