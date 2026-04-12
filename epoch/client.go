// Package epoch wraps the epoch registry client behind a SnapshotRegistry interface for testability.
package epoch

import (
	"context"
	"errors"

	"github.com/cocoonstack/epoch/registryclient"
)

// SnapshotRegistry is the subset of epoch's HTTP API the operator needs.
type SnapshotRegistry interface {
	// HasManifest reports whether (name, tag) exists. Missing returns (false, nil).
	HasManifest(ctx context.Context, name, tag string) (bool, error)
	// DeleteManifest removes the manifest at (name, tag).
	DeleteManifest(ctx context.Context, name, tag string) error
}

var _ SnapshotRegistry = (*Client)(nil)

// Client adapts the upstream epoch registry client to SnapshotRegistry.
type Client struct {
	inner *registryclient.Client
}

// New creates a Client that talks to the epoch registry at baseURL.
func New(baseURL, token string) *Client {
	return &Client{inner: registryclient.New(baseURL, token)}
}

// HasManifest folds ErrManifestNotFound into (false, nil); other errors propagate.
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

// DeleteManifest removes the manifest identified by (name, tag).
func (c *Client) DeleteManifest(ctx context.Context, name, tag string) error {
	return c.inner.DeleteManifest(ctx, name, tag)
}
