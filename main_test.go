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

func TestEnvInt(t *testing.T) {
	cases := []struct {
		name string
		set  string
		want int
	}{
		{"unset falls back", "", 4},
		{"valid value wins", "8", 8},
		{"invalid falls back", "many", 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("TEST_CONCURRENCY", c.set)
			if got := envInt("TEST_CONCURRENCY", 4); got != c.want {
				t.Errorf("envInt = %d, want %d", got, c.want)
			}
		})
	}
}
