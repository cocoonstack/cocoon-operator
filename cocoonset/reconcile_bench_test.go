package cocoonset

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
)

const (
	benchCocoonSets    = 24
	benchRegistryDelay = 5 * time.Millisecond
)

// BenchmarkReconcileThroughput drives independent CocoonSets whose reconciles
// each block on one fixed-latency registry probe, mirroring how the controller
// worker pool overlaps those waits. Concurrency 1 is the pre-change behavior.
func BenchmarkReconcileThroughput(b *testing.B) {
	for _, concurrency := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
			var wall time.Duration
			var latencies []time.Duration
			for b.Loop() {
				b.StopTimer()
				r, sets := newBenchFixture(b)
				b.StartTimer()

				start := time.Now()
				latencies = append(latencies, runReconciles(b, r, sets, concurrency)...)
				wall += time.Since(start)
			}
			slices.Sort(latencies)
			b.ReportMetric(float64(wall.Milliseconds())/float64(b.N), "wall-ms/op")
			b.ReportMetric(float64(percentile(latencies, 95).Milliseconds()), "p95-ms")
		})
	}
}

// runReconciles drains sets through a pool of `concurrency` workers, the shape
// controller-runtime gives MaxConcurrentReconciles, and returns per-reconcile
// latencies.
func runReconciles(b *testing.B, r *Reconciler, sets []*cocoonv1.CocoonSet, concurrency int) []time.Duration {
	b.Helper()
	queue := make(chan *cocoonv1.CocoonSet, len(sets))
	for _, cs := range sets {
		queue <- cs
	}
	close(queue)

	latencies := make([]time.Duration, len(sets))
	var idx atomic.Int32
	var wg sync.WaitGroup
	for range concurrency {
		wg.Go(func() {
			for cs := range queue {
				start := time.Now()
				if _, err := r.Reconcile(context.Background(), reqFor(cs)); err != nil {
					b.Errorf("Reconcile %s: %v", cs.Name, err)
				}
				latencies[idx.Add(1)-1] = time.Since(start)
			}
		})
	}
	wg.Wait()
	return latencies
}

// newBenchFixture builds independent CocoonSets each missing its main agent and
// carrying a hibernated CR, so every reconcile pays exactly one registry probe.
func newBenchFixture(b *testing.B) (*Reconciler, []*cocoonv1.CocoonSet) {
	b.Helper()
	scheme := testScheme(b)
	objs := make([]client.Object, 0, benchCocoonSets*2)
	sets := make([]*cocoonv1.CocoonSet, 0, benchCocoonSets)
	for i := range benchCocoonSets {
		cs := &cocoonv1.CocoonSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:       fmt.Sprintf("bench-%d", i),
				Namespace:  "ns",
				UID:        types.UID(fmt.Sprintf("uid-%d", i)),
				Finalizers: []string{finalizerName},
			},
			Spec: cocoonv1.CocoonSetSpec{
				Agent: cocoonv1.AgentSpec{Image: "ghcr.io/cocoonstack/cocoon/ubuntu:24.04"},
			},
		}
		sets = append(sets, cs)
		objs = append(objs, cs, hibernatedFor(cs))
	}
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&cocoonv1.CocoonSet{}).
		Build()
	return &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{delay: benchRegistryDelay}}, sets
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := min(len(sorted)*p/100, len(sorted)-1)
	return sorted[i]
}
