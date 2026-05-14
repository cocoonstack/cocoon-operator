package cocoonset

import (
	"testing"
	"time"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
)

func TestRebuildHistoryRoundTrip(t *testing.T) {
	cs := &cocoonv1.CocoonSet{}
	cs.Spec.Agent.Replicas = 3
	in := map[int32]rebuildEntry{
		1: {Count: 2, LastDeleted: time.Date(2026, 5, 14, 1, 0, 0, 0, time.UTC)},
		2: {Count: 1, LastDeleted: time.Date(2026, 5, 14, 1, 0, 30, 0, time.UTC)},
	}
	enc, err := encodeRebuildHistory(cs.Spec.Agent.Replicas, in)
	if err != nil {
		t.Fatalf("encodeRebuildHistory: %v", err)
	}
	cs.Annotations = map[string]string{annotationRebuildHistory: enc}
	got := readRebuildHistory(cs)
	if got[1].Count != 2 || got[2].Count != 1 {
		t.Fatalf("round-trip lost counts: %+v", got)
	}
}

func TestRebuildHistoryGarbageCollectsStaleSlots(t *testing.T) {
	in := map[int32]rebuildEntry{
		1: {Count: 1},
		2: {Count: 2},
		7: {Count: 3}, // slot beyond Replicas, must be pruned
	}
	enc, err := encodeRebuildHistory(2, in)
	if err != nil {
		t.Fatalf("encodeRebuildHistory: %v", err)
	}
	cs := &cocoonv1.CocoonSet{}
	cs.Annotations = map[string]string{annotationRebuildHistory: enc}
	got := readRebuildHistory(cs)
	if _, ok := got[7]; ok {
		t.Fatalf("expected slot 7 pruned, got %+v", got)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 surviving slots, got %d: %+v", len(got), got)
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
		if got := backoffDelay(tc.count); got != tc.want {
			t.Errorf("backoffDelay(%d) = %s, want %s", tc.count, got, tc.want)
		}
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
	// Round-trip via encodeRebuildHistory must not panic on the returned map.
	got[1] = rebuildEntry{Count: 1}
	if _, err := encodeRebuildHistory(2, got); err != nil {
		t.Fatalf("encodeRebuildHistory on normalized null payload: %v", err)
	}
}
