package hibernation

import (
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

func TestReconcileHibernateProbeErrorsKeepDeadlineAcrossRetries(t *testing.T) {
	ctx := t.Context()
	key := types.NamespacedName{Namespace: "ns", Name: "demo-0"}
	hib := &cocoonv1.CocoonHibernation{
		Name:       key.Name,
		Namespace:  key.Namespace,
		Generation: 1,
		Finalizers: []string{finalizerName},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireHibernate,
			PodRef: cocoonv1.HibernationPodRef{Name: key.Name},
		},
	}
	pod := &corev1.Pod{Name: key.Name, Namespace: key.Namespace}
	(&meta.VMSpec{VMName: "vk-ns.demo-0", Managed: true}).Apply(pod)
	meta.HibernateState(true).Apply(pod)
	meta.StampCocoonSetGeneration(pod, 1)
	meta.LifecycleStatus{State: meta.LifecycleStateHibernated, ObservedGeneration: 1}.Apply(pod)

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib, pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	reg := &fakeRegistry{manifestErr: errors.New("registry unavailable")}
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}
	req := ctrl.Request{NamespacedName: key}

	for cycle := range 2 {
		res, err := r.Reconcile(ctx, req)
		if !errors.Is(err, reg.manifestErr) {
			t.Fatalf("cycle %d: reconcile error = %v, want registry error", cycle, err)
		}
		if res != (ctrl.Result{}) {
			t.Errorf("cycle %d: result = %+v, want zero result with error", cycle, res)
		}
		if err := cli.Get(ctx, key, hib); err != nil {
			t.Fatalf("get hibernation: %v", err)
		}
		if hib.Status.Phase != cocoonv1.CocoonHibernationPhaseHibernating {
			t.Fatalf("cycle %d: phase = %q, want Hibernating", cycle, hib.Status.Phase)
		}
		ready := apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady)
		if ready == nil || ready.LastTransitionTime.IsZero() || time.Since(ready.LastTransitionTime.Time) >= hibernateTimeout {
			t.Fatalf("cycle %d: Ready = %+v, want a fresh deadline", cycle, ready)
		}

		started := metav1.NewTime(time.Now().Add(-hibernateTimeout / 2).Truncate(time.Second))
		ready.LastTransitionTime = started
		if err := cli.Status().Update(ctx, hib); err != nil {
			t.Fatalf("set in-progress deadline: %v", err)
		}
		if _, err := r.Reconcile(ctx, req); !errors.Is(err, reg.manifestErr) {
			t.Fatalf("retry error = %v, want registry error", err)
		}
		if err := cli.Get(ctx, key, hib); err != nil {
			t.Fatalf("get hibernation after retry: %v", err)
		}
		ready = apimeta.FindStatusCondition(hib.Status.Conditions, commonk8s.ConditionTypeReady)
		if ready == nil || !ready.LastTransitionTime.Equal(&started) {
			t.Fatalf("cycle %d: Ready = %+v, want deadline preserved at %v", cycle, ready, started)
		}

		ready.LastTransitionTime = metav1.NewTime(time.Now().Add(-2 * hibernateTimeout))
		if err := cli.Status().Update(ctx, hib); err != nil {
			t.Fatalf("expire deadline: %v", err)
		}
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("expired probe error must become Failed: %v", err)
		}
		if err := cli.Get(ctx, key, hib); err != nil {
			t.Fatalf("get hibernation after timeout: %v", err)
		}
		if hib.Status.Phase != cocoonv1.CocoonHibernationPhaseFailed {
			t.Fatalf("cycle %d: phase = %q, want Failed", cycle, hib.Status.Phase)
		}
	}

	reg.manifestErr = nil
	reg.manifestPresent = true
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile after registry recovery: %v", err)
	}
	if err := cli.Get(ctx, key, hib); err != nil {
		t.Fatalf("get hibernation after registry recovery: %v", err)
	}
	if hib.Status.Phase != cocoonv1.CocoonHibernationPhaseHibernated {
		t.Errorf("phase = %q, want Hibernated after registry recovery", hib.Status.Phase)
	}
}
