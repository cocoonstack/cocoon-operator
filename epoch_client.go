package main

import (
	"context"

	"github.com/cocoonstack/epoch/registryclient"
)

// SnapshotRegistry is the subset of epoch's HTTP API the operator
// needs. Defined locally so reconciler tests can substitute fakes
// without importing anything from epoch.
type SnapshotRegistry interface {
	// GetManifest returns the raw manifest bytes and content type
	// for the given (name, reference). Used to probe whether a
	// hibernation snapshot has landed.
	GetManifest(ctx context.Context, name, reference string) ([]byte, string, error)

	// DeleteManifest removes the manifest pointer at (name,
	// reference). Used during CocoonSet deletion when the snapshot
	// policy says we should garbage-collect snapshots.
	DeleteManifest(ctx context.Context, name, reference string) error
}

// Compile-time guarantee that *registryclient.Client satisfies the
// SnapshotRegistry interface. If epoch ever changes the signatures,
// the build breaks here instead of inside a reconciler.
var _ SnapshotRegistry = (*registryclient.Client)(nil)

// newEpochClient builds the SnapshotRegistry the operator uses at
// runtime. baseURL and token are sourced from EPOCH_URL and
// EPOCH_TOKEN respectively in main.
func newEpochClient(baseURL, token string) SnapshotRegistry {
	return registryclient.New(baseURL, token)
}
