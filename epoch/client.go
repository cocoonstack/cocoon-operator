// Package epoch wraps the cocoonstack/epoch registry client and
// exposes the small SnapshotRegistry surface the operator's
// reconcilers consume. Defining the interface here means each
// reconciler can take a SnapshotRegistry without depending on the
// concrete epoch client, which keeps the reconciler tests free of any
// epoch import.
package epoch

import (
	"context"

	"github.com/cocoonstack/epoch/registryclient"
)

// SnapshotRegistry is the subset of epoch's HTTP API the operator
// needs. Defined here so reconciler tests can substitute fakes
// without importing anything from epoch.
type SnapshotRegistry interface {
	// HasManifest reports whether (name, tag) currently exists in
	// the registry. A missing manifest is not an error: the
	// hibernation reconciler polls in a loop and treats absence as
	// "not yet pushed", so any error from the underlying probe is
	// folded into a (false, nil) response.
	HasManifest(ctx context.Context, name, tag string) (bool, error)

	// DeleteManifest removes the manifest pointer at (name, tag).
	// Used during CocoonSet deletion when the snapshot policy says
	// we should garbage-collect snapshots, and during wake to drop
	// the hibernate snapshot tag so a future hibernate has a clean
	// slate.
	DeleteManifest(ctx context.Context, name, tag string) error
}

// Compile-time guarantee that *Client satisfies SnapshotRegistry.
// If the upstream epoch client ever changes its signatures, the
// build breaks here instead of inside a reconciler.
var _ SnapshotRegistry = (*Client)(nil)

// Client is the SnapshotRegistry implementation backed by the
// upstream cocoonstack/epoch registry client. It exists as a thin
// adapter so the operator code never imports epoch's loose
// `[]byte, contentType` GetManifest signature directly — narrowing
// to a boolean keeps the reconciler easy to fake.
type Client struct {
	inner *registryclient.Client
}

// New constructs a Client from baseURL and bearer token. The
// underlying epoch client tolerates an empty baseURL by falling back
// to its own default; we pass through whatever the operator was
// configured with.
func New(baseURL, token string) *Client {
	return &Client{inner: registryclient.New(baseURL, token)}
}

// HasManifest implements SnapshotRegistry. epoch's GetManifest folds
// HTTP-level not-found into the same error channel as transport
// failures, so this implementation cannot distinguish them and
// returns (false, nil) for any error — see the SnapshotRegistry doc
// for the polling rationale.
func (c *Client) HasManifest(ctx context.Context, name, tag string) (bool, error) {
	if _, _, err := c.inner.GetManifest(ctx, name, tag); err != nil {
		return false, nil
	}
	return true, nil
}

// DeleteManifest removes a tag from the registry. The upstream
// signature uses "reference" for the tag/digest position; we expose
// it as "tag" because every operator caller passes a tag.
func (c *Client) DeleteManifest(ctx context.Context, name, tag string) error {
	return c.inner.DeleteManifest(ctx, name, tag)
}
