package main

import (
	"context"

	"github.com/projecteru2/core/log"
	"k8s.io/apimachinery/pkg/api/resource"
)

// mustParseQuantity parses a resource quantity string, returning a zero quantity on error.
func mustParseQuantity(ctx context.Context, s string) resource.Quantity {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		log.WithFunc("mustParseQuantity").Warnf(ctx, "invalid quantity %q: %v", s, err)
		return resource.Quantity{}
	}
	return q
}
