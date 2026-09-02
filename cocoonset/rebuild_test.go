package cocoonset

import (
	"strconv"
	"testing"
	"time"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

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

func TestBudgetExhaustedRequiresParkedEntry(t *testing.T) {
	cs := &cocoonv1.CocoonSet{}
	cs.Name = "demo"
	cs.Generation = 3
	cs.Spec.Agent.Replicas = 1
	cases := []struct {
		name  string
		entry rebuildEntry
		want  bool
	}{
		{"count at budget awaiting its replacement", rebuildEntry{Count: maxRebuildAttempts, Generation: 3}, false},
		{"parked at this generation", rebuildEntry{Count: maxRebuildAttempts, Generation: 3, Parked: true}, true},
		{"parked at an older generation", rebuildEntry{Count: maxRebuildAttempts, Generation: 2, Parked: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := encodeRebuildHistory(cs, map[string]rebuildEntry{"demo-0": tc.entry})
			if err != nil {
				t.Fatalf("encodeRebuildHistory: %v", err)
			}
			cs.Annotations = map[string]string{annotationRebuildHistory: enc}
			if got := budgetExhausted(cs, "demo-0"); got != tc.want {
				t.Fatalf("budgetExhausted = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRebuildPodPastBudgetParksNameAndPod(t *testing.T) {
	scheme := testScheme(t)
	cs := newRebuildCS(t, rebuildEntry{Count: maxRebuildAttempts, Generation: 3})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-tb", Namespace: "ns"}}
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs, pod).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}

	deleted, wait, err := r.rebuildPod(t.Context(), log.WithFunc("test"), cs, pod, "spec drifted")
	if err != nil || deleted || wait != 0 {
		t.Fatalf("rebuildPod = (%v, %v, %v), want (false, 0, nil)", deleted, wait, err)
	}
	if !budgetExhausted(cs, "demo-tb") {
		t.Fatal("parked name not exhausted in the mirrored history")
	}
	var stored cocoonv1.CocoonSet
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo"}, &stored); err != nil {
		t.Fatalf("get CocoonSet: %v", err)
	}
	if entry := readRebuildHistory(&stored)["demo-tb"]; !entry.Parked || entry.Count != maxRebuildAttempts {
		t.Fatalf("persisted entry = %+v, want parked at count %d", entry, maxRebuildAttempts)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-tb"}, &got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got.Annotations[annotationDeadLetter] != "3" {
		t.Fatalf("dead-letter annotation = %q, want 3", got.Annotations[annotationDeadLetter])
	}
}

func TestRebuildPodFourthDeleteKeepsNameRecreatable(t *testing.T) {
	scheme := testScheme(t)
	cs := newRebuildCS(t, rebuildEntry{Count: maxRebuildAttempts - 1, Generation: 3, LastDeleted: time.Now().Add(-time.Minute)})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-tb", Namespace: "ns"}}
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs, pod).Build()
	r := &Reconciler{Client: cli, Scheme: scheme}

	deleted, wait, err := r.rebuildPod(t.Context(), log.WithFunc("test"), cs, pod, "spec drifted")
	if err != nil || !deleted || wait != 0 {
		t.Fatalf("rebuildPod = (%v, %v, %v), want (true, 0, nil)", deleted, wait, err)
	}
	if budgetExhausted(cs, "demo-tb") {
		t.Fatal("name exhausted before its final replacement was created")
	}
	if entry := readRebuildHistory(cs)["demo-tb"]; entry.Count != maxRebuildAttempts || entry.Parked {
		t.Fatalf("entry after the fourth delete = %+v", entry)
	}
	var got corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-tb"}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("pod after the fourth delete: err=%v, want NotFound", err)
	}
}

func newRebuildCS(t *testing.T, entry rebuildEntry) *cocoonv1.CocoonSet {
	t.Helper()
	cs := &cocoonv1.CocoonSet{}
	cs.Name, cs.Namespace, cs.Generation = "demo", "ns", 3
	cs.Spec.Toolboxes = []cocoonv1.ToolboxSpec{{Name: "tb"}}
	enc, err := encodeRebuildHistory(cs, map[string]rebuildEntry{"demo-tb": entry})
	if err != nil {
		t.Fatalf("encodeRebuildHistory: %v", err)
	}
	cs.Annotations = map[string]string{annotationRebuildHistory: enc}
	return cs
}
