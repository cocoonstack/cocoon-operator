package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/projecteru2/core/log"
)

func TestCRSinkLine(t *testing.T) {
	cases := []struct {
		name string
		base []any
		kvs  []any
		want string
	}{
		{"no pairs", nil, nil, "msg"},
		{"call pairs", nil, []any{"controller", "cocoonset"}, "msg controller=cocoonset"},
		{"accumulated then call", []any{"ns", "e2e"}, []any{"pod", "demo-0"}, "msg ns=e2e pod=demo-0"},
		{"dangling key dropped", nil, []any{"lone"}, "msg"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &crSink{ctx: t.Context(), kv: c.base}
			if got := s.line("msg", c.kvs); got != c.want {
				t.Errorf("line = %q, want %q", got, c.want)
			}
		})
	}
}

func TestCRSinkWithNameAndValuesDoNotMutateParent(t *testing.T) {
	parent := &crSink{ctx: t.Context(), name: "controller-runtime"}
	child := parent.WithName("manager").(*crSink)
	grandchild := child.WithValues("k", "v").(*crSink)
	if parent.name != "controller-runtime" || len(parent.kv) != 0 {
		t.Errorf("parent mutated: name=%q kv=%v", parent.name, parent.kv)
	}
	if child.name != "controller-runtime.manager" {
		t.Errorf("child name = %q", child.name)
	}
	if grandchild.line("m", nil) != "m k=v" {
		t.Errorf("grandchild line = %q", grandchild.line("m", nil))
	}
}

func TestCRSinkEnabledOnlyV0(t *testing.T) {
	s := &crSink{ctx: t.Context()}
	if !s.Enabled(0) || s.Enabled(1) || s.Enabled(4) {
		t.Error("Enabled must pass V(0) only")
	}
}

func TestCRSinkErrorKeepsTheErrorLevel(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want string
	}{
		{"nil err", nil, `"error":"Update event has no old object to update"`},
		{"err", errors.New("boom"), `"error":"boom"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLog(t)
			logr.New(&crSink{ctx: t.Context()}).WithName("predicate").Error(tt.err, "Update event has no old object to update", "event", "update")
			got := buf.String()
			for _, part := range []string{`"level":"error"`, `"func":"predicate"`, `"message":"Update event has no old object to update event=update"`, tt.want} {
				if !strings.Contains(got, part) {
					t.Fatalf("missing %s in %s", part, got)
				}
			}
		})
	}
}

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	global := log.GetGlobalLogger()
	prev := *global
	var buf bytes.Buffer
	*global = prev.Output(&buf)
	t.Cleanup(func() { *global = prev })
	return &buf
}
