package main

import (
	"context"

	"github.com/projecteru2/core/log"
	"k8s.io/apimachinery/pkg/api/resource"
)

// getMap extracts a map[string]any from a nested unstructured map.
func getMap(obj map[string]any, key string) map[string]any {
	if v, ok := obj[key]; ok {
		if m, ok := v.(map[string]any); ok {
			return m
		}
	}
	return map[string]any{}
}

// getSlice extracts a []any from a nested unstructured map.
func getSlice(obj map[string]any, key string) []any {
	if v, ok := obj[key]; ok {
		if s, ok := v.([]any); ok {
			return s
		}
	}
	return nil
}

// getInt64Value returns an int64 from a map, delegating to toInt64.
func getInt64Value(m map[string]any, key string) (int64, bool) {
	if m == nil {
		return 0, false
	}
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	return toInt64(v), true
}

// stringDefault extracts a string from an unstructured map, returning fallback if empty.
func stringDefault(m map[string]any, key, fallback string) string {
	if v, _ := m[key].(string); v != "" {
		return v
	}
	return fallback
}

// toInt64 converts an unstructured numeric value to int64.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// mustParseQuantity parses a resource quantity string, returning a zero quantity on error.
func mustParseQuantity(ctx context.Context, s string) resource.Quantity {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		log.WithFunc("mustParseQuantity").Warnf(ctx, "invalid quantity %q: %v", s, err)
		return resource.Quantity{}
	}
	return q
}
