package cocoonset

import (
	"errors"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

func TestDeleteRetriesHibernateCleanupBeforeSameNameRecreate(t *testing.T) {
	for _, probe := range []bool{true, false} {
		name := "delete refused"
		if probe {
			name = "probe failed"
		}
		t.Run(name, func(t *testing.T) {
			scheme := testScheme(t)
			cs := newCocoonSet("demo")
			cs.Finalizers = []string{finalizerName}
			vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, 0)
			cs.Status.Agents = []cocoonv1.AgentStatus{{Slot: 0, VMName: vmName}}
			tag := vmName + ":" + meta.HibernateSnapshotTag
			reg := &fakeRegistry{present: map[string]bool{tag: true}, images: map[string]string{tag: cs.Spec.Agent.Image}}
			failure := errors.New("registry temporarily unavailable")
			if probe {
				reg.probeErr = failure
			} else {
				reg.deleteErr = failure
			}
			cli := ctrlfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cs).WithObjects(cs).Build()
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
			r = &Reconciler{Client: cli, Scheme: scheme, Registry: reg}
			if _, err := r.Reconcile(t.Context(), reqFor(cs)); err != nil {
				t.Fatalf("retry after registry recovery: %v", err)
			}
			if err := cli.Get(t.Context(), client.ObjectKeyFromObject(cs), &pending); !apierrors.IsNotFound(err) {
				t.Fatalf("completed deletion = %v, want NotFound", err)
			}
			if reg.present[tag] {
				t.Fatal("hibernate tag survived completed deletion")
			}
			fresh := newCocoonSet("demo")
			fresh.UID = "successor-uid"
			if err := cli.Create(t.Context(), fresh); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := r.Reconcile(t.Context(), reqFor(fresh)); err != nil {
					t.Fatalf("create successor: %v", err)
				}
			}
			var pod corev1.Pod
			if err := cli.Get(t.Context(), client.ObjectKey{Namespace: fresh.Namespace, Name: "demo-0"}, &pod); err != nil {
				t.Fatal(err)
			}
			if meta.ParseVMSpec(&pod).VMName != vmName || meta.ReadRestoreFromHibernate(&pod) || reg.present[tag] {
				t.Fatalf("same-name successor must boot fresh: annotations=%v tag=%v", pod.Annotations, reg.present[tag])
			}
		})
	}
}

func TestDeleteKeepsLatestCleanupBestEffort(t *testing.T) {
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
