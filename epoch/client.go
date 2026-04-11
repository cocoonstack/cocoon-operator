// Package epoch wraps the cocoonstack/epoch registry client and
// exposes the small SnapshotRegistry surface the operator's
// reconcilers consume. Defining the interface here means each
// reconciler can take a SnapshotRegistry without depending on the
// concrete epoch client, which keeps the reconciler tests free of any
// epoch import.
package epoch

import (
	"context"
	"errors"

	"github.com/cocoonstack/epoch/registryclient"
)

// SnapshotRegistry is the subset of epoch's HTTP API the operator
// needs. Defined here so reconciler tests can substitute fakes
// without importing anything from epoch.
type SnapshotRegistry interface {
	// HasManifest reports whether (name, tag) currently exists in
	// the registry. A missing manifest returns (false, nil); any
	// other failure (transport, server error, auth) surfaces as a
	// non-nil error so the caller can decide whether to requeue or
	// mark the parent CR as Failed.
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

// HasManifest implements SnapshotRegistry. It uses
// errors.Is(err, registryclient.ErrManifestNotFound) to recognize
// the "not yet pushed" case and fold it into (false, nil); every
// other error — transport failures, auth errors, 5xx — propagates
// so the hibernation reconciler can surface it on the parent CR's
// status instead of silently retrying forever.
func (c *Client) HasManifest(ctx context.Context, name, tag string) (bool, error) {
	_, _, err := c.inner.GetManifest(ctx, name, tag)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, registryclient.ErrManifestNotFound):
		return false, nil
	default:
		return false, err
	}
}

// DeleteManifest removes a tag from the registry. The upstream
// signature uses "reference" for the tag/digest position; we expose
// it as "tag" because every operator caller passes a tag.
func (c *Client) DeleteManifest(ctx context.Context, name, tag string) error {
	return c.inner.DeleteManifest(ctx, name, tag)
}
