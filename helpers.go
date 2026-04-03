package main

import (
	"context"

	"github.com/projecteru2/core/log"
	"k8s.io/apimachinery/pkg/api/resource"
)

// mustParseQuantity returns false and logs a warning when s is not a valid quantity.
func mustParseQuantity(ctx context.Context, s string) (resource.Quantity, bool) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		log.WithFunc("mustParseQuantity").Warnf(ctx, "invalid quantity %q: %v", s, err)
		return resource.Quantity{}, false
	}
	return q, true
}
