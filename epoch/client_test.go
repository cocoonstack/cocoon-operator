package epoch

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHasManifestNotFoundIsFolded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	present, err := c.HasManifest(t.Context(), "demo", "hibernate")
	if err != nil {
		t.Fatalf("404 must not surface as error, got %v", err)
	}
	if present {
		t.Errorf("404 must report present=false")
	}
}

func TestHasManifestServerErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	present, err := c.HasManifest(t.Context(), "demo", "hibernate")
	if err == nil {
		t.Fatalf("500 must surface as error so reconciler can mark Failed")
	}
	if present {
		t.Errorf("500 must report present=false")
	}
}

func TestHasManifestOKReportsPresent(t *testing.T) {
	const body = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		_, _ = strings.NewReader(body).WriteTo(w)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	present, err := c.HasManifest(t.Context(), "demo", "hibernate")
	if err != nil {
		t.Fatalf("OK must not surface as error, got %v", err)
	}
	if !present {
		t.Errorf("OK must report present=true")
	}
}
