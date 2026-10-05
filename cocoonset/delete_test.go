package cocoonset

import (
	"errors"
	"slices"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

func TestReconcileDeleteRetriesHibernateCleanup(t *testing.T) {
	failure := errors.New("registry temporarily unavailable")
	for _, tc := range []struct {
		name      string
		probeErr  error
		deleteErr error
	}{
		{name: "probe failed", probeErr: failure},
		{name: "delete refused", deleteErr: failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := testScheme(t)
			cs := newCocoonSet("demo")
			cs.Finalizers = []string{finalizerName}
			vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, 0)
			cs.Status.Agents = []cocoonv1.AgentStatus{{Slot: 0, VMName: vmName}}
			tag := vmName + ":" + meta.HibernateSnapshotTag
			reg := &fakeRegistry{present: map[string]bool{tag: true}, probeErr: tc.probeErr, deleteErr: tc.deleteErr}
			cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs).Build()
			r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}
			if err := cli.Delete(t.Context(), cs); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(t.Context(), reqFor(cs)); !errors.Is(err, failure) {
				t.Fatalf("delete error = %v, want registry failure", err)
			}
			var pending cocoonv1.CocoonSet
			if err := cli.Get(t.Context(), client.ObjectKeyFromObject(cs), &pending); err != nil {
				t.Fatalf("set must survive failed cleanup: %v", err)
			}
			if !slices.Contains(pending.Finalizers, finalizerName) || pending.Annotations[annotationDeleteVMNames] != vmName || !reg.present[tag] {
				t.Fatalf("cleanup debt lost: finalizers=%v annotations=%v tag=%v", pending.Finalizers, pending.Annotations, reg.present[tag])
			}
			reg.probeErr, reg.deleteErr = nil, nil
			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
				t.Fatalf("retry after registry recovery: %v", err)
			}
			if err := cli.Get(t.Context(), client.ObjectKeyFromObject(cs), &pending); !apierrors.IsNotFound(err) {
				t.Fatalf("completed deletion = %v, want NotFound", err)
			}
			if reg.present[tag] {
				t.Fatal("hibernate tag survived completed deletion")
			}
		})
	}
}

func TestReconcileDeleteKeepsLatestCleanupBestEffort(t *testing.T) {
	scheme := testScheme(t)
	cs := newCocoonSet("demo")
	cs.Finalizers = []string{finalizerName}
	cs.Spec.SnapshotPolicy = cocoonv1.SnapshotPolicyNever
	vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, 0)
	cs.Status.Agents = []cocoonv1.AgentStatus{{Slot: 0, VMName: vmName}}
	tag := vmName + ":" + meta.DefaultSnapshotTag
	reg := &fakeRegistry{present: map[string]bool{tag: true}, deleteErr: errors.New("latest delete refused")}
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(cs).Build()
	r := &Reconciler{Client: cli, Scheme: scheme, Registry: reg}
	if err := cli.Delete(t.Context(), cs); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
		t.Fatalf("latest cleanup must not block deletion: %v", err)
	}
	var gone cocoonv1.CocoonSet
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(cs), &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("completed deletion = %v, want NotFound", err)
	}
}
