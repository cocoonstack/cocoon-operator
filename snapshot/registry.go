// Package snapshot contains the shared snapshot-registry interface used by
// both the CocoonSet and CocoonHibernation reconcilers.
package snapshot

import "context"

// Registry is the subset of epoch's HTTP API the operator reconcilers need.
// *registryclient.Client from the epoch module satisfies it natively; tests
// swap in fakes.
type Registry interface {
	HasManifest(ctx context.Context, name, reference string) (bool, error)
	DeleteManifest(ctx context.Context, name, reference string) error
}
