package cocoonset

import (
	"strconv"
	"testing"
	"time"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
)

func TestRebuildHistoryRoundTrip(t *testing.T) {
	cs := &cocoonv1.CocoonSet{}
	cs.Name = "demo"
	cs.Spec.Agent.Replicas = 3
	in := map[string]rebuildEntry{
		"demo-1": {Count: 2, LastDeleted: time.Date(2026, 5, 14, 1, 0, 0, 0, time.UTC)},
		"demo-2": {Count: 1, LastDeleted: time.Date(2026, 5, 14, 1, 0, 30, 0, time.UTC)},
	}
	enc, err := encodeRebuildHistory(cs, in)
	if err != nil {
		t.Fatalf("encodeRebuildHistory: %v", err)
	}
	cs.Annotations = map[string]string{annotationRebuildHistory: enc}
	got := readRebuildHistory(cs)
	if got["demo-1"].Count != 2 || got["demo-2"].Count != 1 {
		t.Fatalf("round-trip lost counts: %+v", got)
	}
}

func TestRebuildHistoryGarbageCollectsStalePods(t *testing.T) {
	cs := &cocoonv1.CocoonSet{}
	cs.Name = "demo"
	cs.Spec.Agent.Replicas = 2
	cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{{Name: "tb"}}
	in := map[string]rebuildEntry{
		"demo-0":    {Count: 1},
		"demo-2":    {Count: 2},
		"demo-7":    {Count: 3},
		"demo-tb":   {Count: 1},
		"demo-gone": {Count: 1},
	}
	enc, err := encodeRebuildHistory(cs, in)
	if err != nil {
		t.Fatalf("encodeRebuildHistory: %v", err)
	}
	cs.Annotations = map[string]string{annotationRebuildHistory: enc}
	got := readRebuildHistory(cs)
	for _, name := range []string{"demo-7", "demo-gone"} {
		if _, ok := got[name]; ok {
			t.Fatalf("expected %s pruned, got %+v", name, got)
		}
	}
	if len(got) != 3 || len(in) != 5 {
		t.Fatalf("expected 3 surviving pods and an untouched input, got %d / %d: %+v", len(got), len(in), got)
	}
}

func TestBackoffDelaySchedule(t *testing.T) {
	cases := []struct {
		count int
		want  time.Duration
	}{
		{0, 0},
		{1, 1 * time.Second},
		{2, 5 * time.Second},
		{3, 30 * time.Second},
		{10, 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(strconv.Itoa(tc.count), func(t *testing.T) {
			if got := backoffDelay(tc.count); got != tc.want {
				t.Errorf("backoffDelay(%d) = %s, want %s", tc.count, got, tc.want)
			}
		})
	}
}

func TestReadRebuildHistoryHandlesCorruptAnnotation(t *testing.T) {
	cs := &cocoonv1.CocoonSet{}
	cs.Annotations = map[string]string{annotationRebuildHistory: "not-json"}
	if got := readRebuildHistory(cs); len(got) != 0 {
		t.Fatalf("corrupt annotation must yield empty history, got %+v", got)
	}
}

func TestReadRebuildHistoryHandlesNullPayload(t *testing.T) {
	cs := &cocoonv1.CocoonSet{}
	cs.Annotations = map[string]string{annotationRebuildHistory: "null"}
	got := readRebuildHistory(cs)
	if got == nil {
		t.Fatal("null payload must yield non-nil map so downstream writes don't panic")
	}
	got["demo-1"] = rebuildEntry{Count: 1}
	if _, err := encodeRebuildHistory(cs, got); err != nil {
		t.Fatalf("encodeRebuildHistory on normalized null payload: %v", err)
	}
}
