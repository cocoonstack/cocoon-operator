package hibernation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

func TestReadyConditionMaps(t *testing.T) {
	cases := []struct {
		phase  cocoonv1.CocoonHibernationPhase
		status metav1.ConditionStatus
	}{
		{cocoonv1.CocoonHibernationPhaseHibernated, metav1.ConditionTrue},
		{cocoonv1.CocoonHibernationPhaseActive, metav1.ConditionTrue},
		{cocoonv1.CocoonHibernationPhaseFailed, metav1.ConditionFalse},
		{cocoonv1.CocoonHibernationPhaseHibernating, metav1.ConditionFalse},
		{cocoonv1.CocoonHibernationPhaseWaking, metav1.ConditionFalse},
		{cocoonv1.CocoonHibernationPhasePending, metav1.ConditionFalse},
	}
	for _, c := range cases {
		t.Run(string(c.phase), func(t *testing.T) {
			cond := readyCondition(c.phase, 1)
			if cond.Status != c.status {
				t.Errorf("phase %q -> status %q, want %q", c.phase, cond.Status, c.status)
			}
			if cond.ObservedGeneration != 1 {
				t.Errorf("observedGeneration: %d", cond.ObservedGeneration)
			}
		})
	}
}

func TestFakeRegistryGetMissing(t *testing.T) {
	r := &fakeRegistry{}
	present, err := r.HasManifest(t.Context(), "x", "hibernate")
	if err != nil {
		t.Fatalf("has: %v", err)
	}
	if present {
		t.Errorf("expected manifest absent")
	}
}

func TestFakeRegistryGetPresent(t *testing.T) {
	r := &fakeRegistry{manifestPresent: true}
	present, err := r.HasManifest(t.Context(), "x", "hibernate")
	if err != nil {
		t.Fatalf("has: %v", err)
	}
	if !present {
		t.Errorf("expected manifest present")
	}
}

func TestFakeRegistryGetErrorPropagates(t *testing.T) {
	r := &fakeRegistry{manifestErr: errors.New("transport boom")}
	if _, err := r.HasManifest(t.Context(), "x", "hibernate"); err == nil {
		t.Errorf("expected transport error to surface")
	}
}

func TestFakeRegistryDeleteRecords(t *testing.T) {
	r := &fakeRegistry{}
	if err := r.DeleteManifest(t.Context(), "x", "hibernate"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !r.deleteCalled {
		t.Errorf("delete was not recorded")
	}
}

// Finalizer add must end the cycle so the next reconcile reads the persisted object.
func TestReconcileAddsFinalizerAndRequeues(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns"},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireHibernate,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
	}
	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	res, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Requeue = false, want true after finalizer add")
	}
	var got cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &got); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if len(got.Finalizers) == 0 || got.Finalizers[0] != finalizerName {
		t.Errorf("finalizers = %v, want [%s]", got.Finalizers, finalizerName)
	}
	if got.Status.Phase != "" {
		t.Errorf("status mutated before finalizer persisted, phase = %q", got.Status.Phase)
	}
}

func TestReconcileDeleteClearsHibernateTagAndFinalizer(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "hib",
			Namespace:         "ns",
			Finalizers:        []string{finalizerName},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec:   cocoonv1.CocoonHibernationSpec{PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"}},
		Status: cocoonv1.CocoonHibernationStatus{VMName: "vk-ns-demo-0"},
	}

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	reg := &fakeRegistry{}
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}

	if err := r.reconcileDelete(t.Context(), hib); err != nil {
		t.Fatalf("reconcileDelete: %v", err)
	}
	if !reg.deleteCalled {
		t.Errorf("DeleteManifest must be called on reconcileDelete")
	}
	// After RemoveFinalizer + Update, the fake client should let the object be gone
	// since the test wrote a DeletionTimestamp and there are no other finalizers.
	var got cocoonv1.CocoonHibernation
	err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &got)
	if err == nil && len(got.Finalizers) != 0 {
		t.Errorf("finalizer must be removed, got %v", got.Finalizers)
	}
}

func TestReconcileDeleteSkipsTagWhenVMNameMissing(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "hib",
			Namespace:         "ns",
			Finalizers:        []string{finalizerName},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: cocoonv1.CocoonHibernationSpec{PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"}},
		// Status.VMName left empty — delete before vk-cocoon ever filled it.
	}

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	reg := &fakeRegistry{}
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}

	if err := r.reconcileDelete(t.Context(), hib); err != nil {
		t.Fatalf("reconcileDelete: %v", err)
	}
	if reg.deleteCalled {
		t.Errorf("DeleteManifest must be skipped when Status.VMName is empty")
	}
}

func TestPodVMNameRoundtrip(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	(&meta.VMSpec{VMName: "vk-ns-demo-0", Managed: true}).Apply(pod)
	if got := meta.ParseVMSpec(pod).VMName; got != "vk-ns-demo-0" {
		t.Errorf("vmName roundtrip: %q", got)
	}
}

// TestReconcileHibernateSurfacesProbeError verifies that transport/server errors
// from HasManifest bubble out instead of being silently polled forever.
func TestReconcileHibernateSurfacesProbeError(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Finalizers: []string{finalizerName}},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireHibernate,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	(&meta.VMSpec{VMName: "vk-ns-demo-0", Managed: true}).Apply(pod)
	// Pre-set so PatchHibernateState short-circuits before the interesting HasManifest call.
	meta.HibernateState(true).Apply(pod)

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib, pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()

	r := &Reconciler{
		Client:   cli,
		Scheme:   scheme,
		Registry: &fakeRegistry{manifestErr: errors.New("transport boom")},
	}

	res, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	})
	if err == nil {
		t.Fatalf("expected probe error to surface from Reconcile, got nil")
	}
	if !errors.Is(err, r.Registry.(*fakeRegistry).manifestErr) {
		t.Errorf("Reconcile err = %v, want wrap of transport boom", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("error return should leave Result zero-valued (let backoff drive requeue), got %+v", res)
	}
}

func TestReconcileHibernateFoldsAbsenceToRequeue(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Finalizers: []string{finalizerName}},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireHibernate,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	(&meta.VMSpec{VMName: "vk-ns-demo-0", Managed: true}).Apply(pod)
	meta.HibernateState(true).Apply(pod)

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib, pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()

	r := &Reconciler{
		Client:   cli,
		Scheme:   scheme,
		Registry: &fakeRegistry{}, // absent, no error
	}
	res, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != requeueInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, requeueInterval)
	}
}

func TestWakeDeadlineExceeded(t *testing.T) {
	oldReady := metav1.Condition{
		Type:               commonk8s.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             conditionReasonPending,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * wakeTimeout)),
	}
	freshReady := metav1.Condition{
		Type:               commonk8s.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             conditionReasonPending,
		LastTransitionTime: metav1.NewTime(time.Now()),
	}
	cases := []struct {
		name string
		hib  *cocoonv1.CocoonHibernation
		want bool
	}{
		{
			name: "not-waking-yet",
			hib:  &cocoonv1.CocoonHibernation{},
			want: false,
		},
		{
			name: "waking-within-budget",
			hib: &cocoonv1.CocoonHibernation{
				Status: cocoonv1.CocoonHibernationStatus{
					Phase:      cocoonv1.CocoonHibernationPhaseWaking,
					Conditions: []metav1.Condition{freshReady},
				},
			},
			want: false,
		},
		{
			name: "waking-past-budget",
			hib: &cocoonv1.CocoonHibernation{
				Status: cocoonv1.CocoonHibernationStatus{
					Phase:      cocoonv1.CocoonHibernationPhaseWaking,
					Conditions: []metav1.Condition{oldReady},
				},
			},
			want: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := phaseDeadlineExceeded(c.hib, cocoonv1.CocoonHibernationPhaseWaking, wakeTimeout); got != c.want {
				t.Errorf("phaseDeadlineExceeded(Waking) = %v, want %v", got, c.want)
			}
		})
	}
}

func TestReconcileWakeFailsOnTimeout(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Finalizers: []string{finalizerName}},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireWake,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
		Status: cocoonv1.CocoonHibernationStatus{
			Phase: cocoonv1.CocoonHibernationPhaseWaking,
			Conditions: []metav1.Condition{{
				Type:               commonk8s.ConditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             conditionReasonPending,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * wakeTimeout)),
			}},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	(&meta.VMSpec{VMName: "vk-ns-demo-0", Managed: true}).Apply(pod)

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib, pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var out cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &out); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if out.Status.Phase != cocoonv1.CocoonHibernationPhaseFailed {
		t.Errorf("phase = %q, want Failed", out.Status.Phase)
	}
}

// TestReconcileWakeRecoversFromFailed verifies that Failed->Waking re-entry
// refreshes LastTransitionTime so the wake deadline does not trip immediately.
func TestReconcileWakeRecoversFromFailed(t *testing.T) {
	staleTime := metav1.NewTime(time.Now().Add(-2 * wakeTimeout))
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Finalizers: []string{finalizerName}},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireWake,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
		Status: cocoonv1.CocoonHibernationStatus{
			Phase: cocoonv1.CocoonHibernationPhaseFailed,
			Conditions: []metav1.Condition{{
				Type:               commonk8s.ConditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             conditionReasonFailed,
				LastTransitionTime: staleTime,
			}},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	(&meta.VMSpec{VMName: "vk-ns-demo-0", Managed: true}).Apply(pod)

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib, pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var out cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &out); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if out.Status.Phase != cocoonv1.CocoonHibernationPhaseWaking {
		t.Fatalf("phase = %q, want Waking (recovery path)", out.Status.Phase)
	}
	ready := apimeta.FindStatusCondition(out.Status.Conditions, commonk8s.ConditionTypeReady)
	if ready == nil {
		t.Fatalf("Ready condition missing after recovery reconcile")
	}
	if !ready.LastTransitionTime.Time.After(staleTime.Time) {
		t.Errorf("LastTransitionTime = %v (stale = %v), want refreshed", ready.LastTransitionTime.Time, staleTime.Time)
	}
	// Second reconcile must not re-fail with the fresh timestamp.
	if _, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	}); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &out); err != nil {
		t.Fatalf("get hib (second): %v", err)
	}
	if out.Status.Phase != cocoonv1.CocoonHibernationPhaseWaking {
		t.Errorf("phase after second reconcile = %q, want Waking (deadline must not re-trip)", out.Status.Phase)
	}
}

func TestReconcileWakeClearsHibernateResidueOnFastPath(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Finalizers: []string{finalizerName}},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireWake,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
		Status: cocoonv1.CocoonHibernationStatus{Phase: cocoonv1.CocoonHibernationPhaseWaking},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	(&meta.VMSpec{VMName: "vk-ns-demo-0", Managed: true}).Apply(pod)
	(&meta.VMRuntime{VMID: "vmid-live"}).Apply(pod)
	meta.HibernateState(true).Apply(pod)

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib, pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var outHib cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &outHib); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if outHib.Status.Phase != cocoonv1.CocoonHibernationPhaseActive {
		t.Errorf("phase = %q, want Active (fast-path)", outHib.Status.Phase)
	}

	var outPod corev1.Pod
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "demo-0"}, &outPod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if meta.ReadHibernateState(&outPod) {
		t.Error("hibernate annotation must be cleared on the wake fast-path; still true")
	}
}

func TestHibernateDeadlineExceeded(t *testing.T) {
	staleReady := metav1.Condition{
		Type:               commonk8s.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             conditionReasonPending,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * hibernateTimeout)),
	}
	freshReady := metav1.Condition{
		Type:               commonk8s.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             conditionReasonPending,
		LastTransitionTime: metav1.NewTime(time.Now()),
	}
	cases := []struct {
		name string
		hib  *cocoonv1.CocoonHibernation
		want bool
	}{
		{"not-hibernating-yet", &cocoonv1.CocoonHibernation{}, false},
		{
			"hibernating-within-budget",
			&cocoonv1.CocoonHibernation{Status: cocoonv1.CocoonHibernationStatus{
				Phase: cocoonv1.CocoonHibernationPhaseHibernating, Conditions: []metav1.Condition{freshReady},
			}},
			false,
		},
		{
			"hibernating-past-budget",
			&cocoonv1.CocoonHibernation{Status: cocoonv1.CocoonHibernationStatus{
				Phase: cocoonv1.CocoonHibernationPhaseHibernating, Conditions: []metav1.Condition{staleReady},
			}},
			true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := phaseDeadlineExceeded(c.hib, cocoonv1.CocoonHibernationPhaseHibernating, hibernateTimeout); got != c.want {
				t.Errorf("phaseDeadlineExceeded(Hibernating) = %v, want %v", got, c.want)
			}
		})
	}
}

func TestReconcileHibernateFailsOnTimeout(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Finalizers: []string{finalizerName}},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireHibernate,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
		Status: cocoonv1.CocoonHibernationStatus{
			Phase: cocoonv1.CocoonHibernationPhaseHibernating,
			Conditions: []metav1.Condition{{
				Type:               commonk8s.ConditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             conditionReasonPending,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * hibernateTimeout)),
			}},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	(&meta.VMSpec{VMName: "vk-ns-demo-0", Managed: true}).Apply(pod)
	meta.HibernateState(true).Apply(pod)

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib, pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var out cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &out); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if out.Status.Phase != cocoonv1.CocoonHibernationPhaseFailed {
		t.Errorf("phase = %q, want Failed", out.Status.Phase)
	}
}

// Failed→Hibernating re-entry must refresh LastTransitionTime so the new
// hibernate deadline starts from zero.
func TestReconcileHibernateRecoversFromFailed(t *testing.T) {
	staleTime := metav1.NewTime(time.Now().Add(-2 * hibernateTimeout))
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Finalizers: []string{finalizerName}},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireHibernate,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
		Status: cocoonv1.CocoonHibernationStatus{
			Phase: cocoonv1.CocoonHibernationPhaseFailed,
			Conditions: []metav1.Condition{{
				Type:               commonk8s.ConditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             conditionReasonFailed,
				LastTransitionTime: staleTime,
			}},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	(&meta.VMSpec{VMName: "vk-ns-demo-0", Managed: true}).Apply(pod)

	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib, pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var out cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &out); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if out.Status.Phase != cocoonv1.CocoonHibernationPhaseHibernating {
		t.Fatalf("phase = %q, want Hibernating (recovery path)", out.Status.Phase)
	}
	ready := apimeta.FindStatusCondition(out.Status.Conditions, commonk8s.ConditionTypeReady)
	if ready == nil {
		t.Fatal("Ready condition missing after recovery reconcile")
	}
	if !ready.LastTransitionTime.Time.After(staleTime.Time) {
		t.Errorf("LastTransitionTime = %v (stale = %v), want refreshed", ready.LastTransitionTime.Time, staleTime.Time)
	}
}

func TestSetPhasePatchesObservedGenerationOnSamePhase(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Generation: 7},
		Status: cocoonv1.CocoonHibernationStatus{
			Phase:              cocoonv1.CocoonHibernationPhaseHibernated,
			VMName:             "vk-ns-demo-0",
			ObservedGeneration: 6, // lags by one rev
		},
	}
	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}

	if err := r.setPhase(t.Context(), hib, cocoonv1.CocoonHibernationPhaseHibernated, "vk-ns-demo-0"); err != nil {
		t.Fatalf("setPhase: %v", err)
	}

	var out cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &out); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if out.Status.ObservedGeneration != 7 {
		t.Errorf("ObservedGeneration = %d, want 7 (unchanged-phase reconcile must still surface generation bump)", out.Status.ObservedGeneration)
	}
}

func TestMarkPendingPatchesObservedGenerationOnSameMessage(t *testing.T) {
	msg := "pod ns/demo-0 not yet present"
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Generation: 4},
		Status: cocoonv1.CocoonHibernationStatus{
			Phase:              cocoonv1.CocoonHibernationPhasePending,
			ObservedGeneration: 3, // lags by one rev
			Conditions: []metav1.Condition{{
				Type:    commonk8s.ConditionTypeReady,
				Status:  metav1.ConditionFalse,
				Reason:  conditionReasonPending,
				Message: msg,
			}},
		},
	}
	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme}

	if err := r.markPending(t.Context(), hib, msg); err != nil {
		t.Fatalf("markPending: %v", err)
	}

	var out cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &out); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if out.Status.ObservedGeneration != 4 {
		t.Errorf("ObservedGeneration = %d, want 4 (markPending must patch when generation moved even if message is unchanged)", out.Status.ObservedGeneration)
	}
}

func TestReconcilePendingWhenPodMissing(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Finalizers: []string{finalizerName}},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireHibernate,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
	}
	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	res, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != requeueInterval {
		t.Errorf("RequeueAfter = %v, want %v (missing pod must requeue)", res.RequeueAfter, requeueInterval)
	}

	var out cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &out); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if out.Status.Phase != cocoonv1.CocoonHibernationPhasePending {
		t.Errorf("phase = %q, want Pending (must not dead-end on Failed)", out.Status.Phase)
	}
}

func TestReconcilePendingWhenPodMissingVMName(t *testing.T) {
	hib := &cocoonv1.CocoonHibernation{
		ObjectMeta: metav1.ObjectMeta{Name: "hib", Namespace: "ns", Finalizers: []string{finalizerName}},
		Spec: cocoonv1.CocoonHibernationSpec{
			Desire: cocoonv1.HibernationDesireHibernate,
			PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
		},
	}
	// Pod exists but vk-cocoon has not yet set the VMName annotation.
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns"}}
	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib, pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: &fakeRegistry{}}

	res, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "hib"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != requeueInterval {
		t.Errorf("RequeueAfter = %v, want %v (missing VMName must requeue)", res.RequeueAfter, requeueInterval)
	}

	var out cocoonv1.CocoonHibernation
	if err := cli.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "hib"}, &out); err != nil {
		t.Fatalf("get hib: %v", err)
	}
	if out.Status.Phase != cocoonv1.CocoonHibernationPhasePending {
		t.Errorf("phase = %q, want Pending", out.Status.Phase)
	}
}

// TestReconcileSerializesCRsTargetingOnePod pins the pod lock: nothing stops two
// CRs from naming one pod with opposing desires, and above one worker they would
// otherwise interleave the shared hibernate annotation and :hibernate tag.
func TestReconcileSerializesCRsTargetingOnePod(t *testing.T) {
	hib := func(name string, desire cocoonv1.HibernationDesire) *cocoonv1.CocoonHibernation {
		return &cocoonv1.CocoonHibernation{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Finalizers: []string{finalizerName}},
			Spec: cocoonv1.CocoonHibernationSpec{
				Desire: desire,
				PodRef: cocoonv1.HibernationPodRef{Name: "demo-0"},
			},
		}
	}
	// Running + a VMID is what drives the wake path all the way to DeleteManifest,
	// so both desires actually reach the registry and can be observed racing.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo-0", Namespace: "ns",
			Annotations: map[string]string{
				meta.AnnotationVMName: "vk-ns-demo-0",
				meta.AnnotationVMID:   "vmid-1",
			},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		}},
	}
	scheme := testScheme(t)
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(hib("a", cocoonv1.HibernationDesireHibernate), hib("b", cocoonv1.HibernationDesireWake), pod).
		WithStatusSubresource(&cocoonv1.CocoonHibernation{}).
		Build()
	reg := &concurrencyProbe{}
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}

	var wg sync.WaitGroup
	for _, name := range []string{"a", "b"} {
		wg.Go(func() {
			_, _ = r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: "ns", Name: name},
			})
		})
	}
	wg.Wait()
	if reg.maxInFlight.Load() > 1 {
		t.Errorf("CRs targeting one pod ran %d registry calls in flight, want serialized", reg.maxInFlight.Load())
	}
}

// concurrencyProbe records the peak number of registry calls in flight.
type concurrencyProbe struct {
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (c *concurrencyProbe) HasManifest(context.Context, string, string) (bool, error) {
	c.enter()
	defer c.inFlight.Add(-1)
	time.Sleep(20 * time.Millisecond)
	return false, nil
}

func (c *concurrencyProbe) DeleteManifest(context.Context, string, string) error {
	c.enter()
	defer c.inFlight.Add(-1)
	time.Sleep(20 * time.Millisecond)
	return nil
}

func (c *concurrencyProbe) enter() {
	if n := c.inFlight.Add(1); n > c.maxInFlight.Load() {
		c.maxInFlight.Store(n)
	}
}

// A nil manager suffices: the guard rejects before mgr is touched.
func TestSetupWithManagerRejectsInvalidConcurrency(t *testing.T) {
	for _, n := range []int{0, -1} {
		if err := (&Reconciler{Concurrency: n}).SetupWithManager(t.Context(), nil); err == nil {
			t.Errorf("concurrency %d must be rejected", n)
		}
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sch := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sch); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cocoonv1.AddToScheme(sch); err != nil {
		t.Fatalf("add cocoonv1 scheme: %v", err)
	}
	return sch
}

type fakeRegistry struct {
	manifestPresent bool
	manifestErr     error
	deleteCalled    bool
	deleteErr       error
}

func (f *fakeRegistry) HasManifest(_ context.Context, _, _ string) (bool, error) {
	if f.manifestErr != nil {
		return false, f.manifestErr
	}
	return f.manifestPresent, nil
}

func (f *fakeRegistry) DeleteManifest(_ context.Context, _, _ string) error {
	f.deleteCalled = true
	return f.deleteErr
}
