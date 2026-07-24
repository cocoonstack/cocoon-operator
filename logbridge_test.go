package main

import (
	"testing"
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

func TestCRSinkErrorNilDoesNotPanic(t *testing.T) {
	// Output capture is impractical — pin no-panic on both the nil and real paths.
	s := &crSink{ctx: t.Context(), name: "controller-runtime"}
	s.Error(nil, "update event has no old object", "type", "pod")
	s.Error(assertErr{}, "real error path")
}

type assertErr struct{}

func (assertErr) Error() string { return "boom" }
