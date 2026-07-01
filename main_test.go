package main

import (
	"testing"

	"github.com/cocoonstack/cocoon-common/oci"
	"github.com/cocoonstack/epoch/registryclient"
)

func TestBuildRegistryBackend(t *testing.T) {
	t.Setenv("EPOCH_CA_CERT", "") // keep the epoch client deterministic on dev machines

	t.Setenv("OCI_REGISTRY", "example.com/proj/repo")
	reg, err := buildRegistry()
	if err != nil {
		t.Fatalf("buildRegistry(OCI): %v", err)
	}
	if _, ok := reg.(*oci.OCIRegistry); !ok {
		t.Fatalf("OCI_REGISTRY set: got %T, want *oci.OCIRegistry", reg)
	}

	t.Setenv("OCI_REGISTRY", "")
	ep, err := buildRegistry()
	if err != nil {
		t.Fatalf("buildRegistry(epoch): %v", err)
	}
	if _, ok := ep.(*registryclient.Client); !ok {
		t.Fatalf("no OCI_REGISTRY: got %T, want *registryclient.Client", ep)
	}
}
