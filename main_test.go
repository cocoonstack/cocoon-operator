package main

import (
	"testing"

	"github.com/cocoonstack/cocoon-common/oci"
)

func TestBuildRegistry(t *testing.T) {
	t.Setenv("OCI_REGISTRY", "example.com/proj/repo")
	reg, err := buildRegistry()
	if err != nil {
		t.Fatalf("buildRegistry: %v", err)
	}
	if _, ok := reg.(*oci.OCIRegistry); !ok {
		t.Fatalf("got %T, want *oci.OCIRegistry", reg)
	}

	t.Setenv("OCI_REGISTRY", "")
	if _, err := buildRegistry(); err == nil {
		t.Fatal("buildRegistry with no OCI_REGISTRY: want error, got nil")
	}
}
