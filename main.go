// cocoon-operator manages Hibernation and CocoonSet CRDs for Cocoon VM pods.
//
// It watches Hibernation CRDs and drives pod hibernate/wake by annotating pods
// with cocoon.cis/hibernate=true. vk-cocoon detects the annotation and
// handles the actual VM snapshot/restore lifecycle.
//
// K8s concepts reused:
//   - Pod stays alive during hibernation (phase=Running, Ready=False, container
//     Waiting "Hibernated"). This prevents RS/STS controllers from recreating it.
//   - Pod-deletion-cost annotation is used to influence which pod gets selected
//     when the operator needs to work with Deployment scale operations.
//   - ConfigMap cocoon-vm-snapshots tracks suspended VM snapshots (managed by vk-cocoon).
//
// Usage:
//
//	# Install CRDs
//	kubectl apply -f deploy/crd.yaml
//	kubectl apply -f deploy/cocoonset-crd.yaml
//
//	# Hibernate a pod
//	kubectl apply -f - <<EOF
//	apiVersion: cocoon.cis/v1alpha1
//	kind: Hibernation
//	metadata:
//	  name: hibernate-bot-1
//	  namespace: prod
//	spec:
//	  podName: sre-agent-xxx
//	  action: hibernate
//	EOF
//
//	# Wake it up
//	kubectl patch hibernation hibernate-bot-1 -n prod --type merge -p '{"spec":{"action":"wake"}}'
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/cocoonstack/cocoon-operator/pkg/k8sutil"
)

// hibGVR is the GroupVersionResource for Hibernation CRDs.
var hibGVR = schema.GroupVersionResource{
	Group:    "cocoon.cis",
	Version:  "v1alpha1",
	Resource: "hibernations",
}

const (
	annHibernate = "cocoon.cis/hibernate"
	annVMName    = "cocoon.cis/vm-name"

	valTrue = "true"
)

// controller holds the Kubernetes clients used by all reconcile loops.
type controller struct {
	clientset *kubernetes.Clientset
	dynClient dynamic.Interface
}

func main() {
	klog.InitFlags(nil)

	config, err := k8sutil.LoadConfig()
	if err != nil {
		klog.Fatalf("k8s config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("clientset: %v", err)
	}
	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		klog.Fatalf("dynamic client: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ctrl := &controller{
		clientset: clientset,
		dynClient: dynClient,
	}

	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, 30*time.Second)

	// Hibernation informer.
	hibInformer := factory.ForResource(hibGVR).Informer()
	_, _ = hibInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint:errcheck // registration handle unused
		AddFunc:    func(obj any) { ctrl.reconcile(ctx, obj) },
		UpdateFunc: func(_, obj any) { ctrl.reconcile(ctx, obj) },
	})

	// CocoonSet informer.
	csInformer := factory.ForResource(csGVR).Informer()
	_, _ = csInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint:errcheck // registration handle unused
		AddFunc: func(obj any) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
			if err := ctrl.reconcileCocoonSet(ctx, u.GetNamespace(), u.GetName()); err != nil {
				klog.Errorf("cocoonset %s/%s: reconcile on add: %v", u.GetNamespace(), u.GetName(), err)
			}
		},
		UpdateFunc: func(_, obj any) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
			if err := ctrl.reconcileCocoonSet(ctx, u.GetNamespace(), u.GetName()); err != nil {
				klog.Errorf("cocoonset %s/%s: reconcile on update: %v", u.GetNamespace(), u.GetName(), err)
			}
		},
	})

	// Pod informer — if a pod's ownerRef points to a CocoonSet, reconcile it.
	podInformer := factory.ForResource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "pods",
	}).Informer()
	_, _ = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint:errcheck // registration handle unused
		UpdateFunc: func(_, obj any) { ctrl.handlePodEvent(ctx, obj) },
		DeleteFunc: func(obj any) { ctrl.handlePodEvent(ctx, obj) },
	})

	// Periodic re-sync to detect vk-cocoon status changes on pods.
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ctrl.resync(ctx, hibInformer.GetStore())
				ctrl.resyncCocoonSets(ctx, csInformer.GetStore())
			}
		}
	}()

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	klog.Info("cocoon-operator started")

	<-ctx.Done()
	klog.Info("cocoon-operator shutting down")
}

// ---------- Hibernation reconcile ----------

func (c *controller) reconcile(ctx context.Context, obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}

	ns := u.GetNamespace()
	name := u.GetName()
	spec := getMap(u.Object, "spec")
	status := getMap(u.Object, "status")

	podName, _ := spec["podName"].(string)
	action, _ := spec["action"].(string)
	phase, _ := status["phase"].(string)

	if podName == "" {
		return
	}
	if action == "" {
		action = "hibernate"
	}

	switch action {
	case "hibernate":
		c.reconcileHibernate(ctx, ns, name, podName, phase)
	case "wake":
		c.reconcileWake(ctx, ns, name, podName, phase)
	}
}

func (c *controller) reconcileHibernate(ctx context.Context, ns, hibName, podName, phase string) {
	if phase == "Hibernated" {
		return
	}

	pod, err := c.clientset.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		c.updateStatus(ctx, ns, hibName, "Failed", fmt.Sprintf("pod not found: %v", err), "")
		return
	}

	vmName := pod.Annotations[annVMName]

	// If vk-cocoon already saved a snapshot for this VM, hibernation is complete.
	if vmName != "" && c.hasSnapshot(ctx, ns, vmName) {
		c.updateStatus(ctx, ns, hibName, "Hibernated", "VM suspended to epoch", vmName)
		return
	}

	// Annotate pod to trigger vk-cocoon hibernate.
	if pod.Annotations[annHibernate] != valTrue {
		c.patchPodAnnotation(ctx, ns, hibName, podName, annHibernate, valTrue, "hibernate")
	}

	if phase != "Hibernating" {
		c.updateStatus(ctx, ns, hibName, "Hibernating", "Waiting for vk-cocoon to snapshot VM", vmName)
	}
}

func (c *controller) reconcileWake(ctx context.Context, ns, hibName, podName, phase string) {
	if phase == "Active" {
		return
	}

	pod, err := c.clientset.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		c.updateStatus(ctx, ns, hibName, "Failed", fmt.Sprintf("pod not found: %v", err), "")
		return
	}

	vmName := pod.Annotations[annVMName]

	// Pod is awake when the hibernate annotation is gone and the snapshot is consumed.
	if pod.Annotations[annHibernate] != valTrue && vmName != "" && !c.hasSnapshot(ctx, ns, vmName) {
		c.updateStatus(ctx, ns, hibName, "Active", "VM restored and running", vmName)
		return
	}

	// Remove hibernate annotation to trigger vk-cocoon wake.
	if pod.Annotations[annHibernate] == valTrue {
		c.patchPodAnnotationNull(ctx, ns, hibName, podName, annHibernate, "wake")
	}

	if phase != "Waking" {
		c.updateStatus(ctx, ns, hibName, "Waking", "Waiting for vk-cocoon to restore VM", vmName)
	}
}

// resync checks all Hibernation CRDs for status transitions.
func (c *controller) resync(ctx context.Context, store cache.Store) {
	for _, obj := range store.List() {
		c.reconcile(ctx, obj)
	}
}

// handlePodEvent checks if a pod is owned by a CocoonSet and reconciles it.
func (c *controller) handlePodEvent(ctx context.Context, obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			u, ok = d.Obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
		} else {
			return
		}
	}

	for _, ref := range u.GetOwnerReferences() {
		if ref.Kind == "CocoonSet" && ref.APIVersion == "cocoon.cis/v1alpha1" {
			if err := c.reconcileCocoonSet(ctx, u.GetNamespace(), ref.Name); err != nil {
				klog.Errorf("cocoonset %s/%s: reconcile on pod event: %v", u.GetNamespace(), ref.Name, err)
			}
			return
		}
	}
}

// resyncCocoonSets reconciles all CocoonSets on timer tick.
func (c *controller) resyncCocoonSets(ctx context.Context, store cache.Store) {
	for _, obj := range store.List() {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		if err := c.reconcileCocoonSet(ctx, u.GetNamespace(), u.GetName()); err != nil {
			klog.Errorf("cocoonset %s/%s: reconcile on resync: %v", u.GetNamespace(), u.GetName(), err)
		}
	}
}

// ---------- Hibernation status ----------

func (c *controller) updateStatus(ctx context.Context, ns, name, phase, message, vmName string) {
	status := map[string]any{
		"phase":              phase,
		"message":            message,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}
	if vmName != "" {
		status["vmName"] = vmName
	}

	if err := c.patchStatus(ctx, hibGVR, ns, name, status); err != nil {
		klog.Errorf("updateStatus %s/%s -> %s: %v", ns, name, phase, err)
	}
}

// patchStatus patches the status subresource of any CRD.
func (c *controller) patchStatus(ctx context.Context, gvr schema.GroupVersionResource, ns, name string, status any) error {
	patch := map[string]any{"status": status}
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	_, err = c.dynClient.Resource(gvr).Namespace(ns).Patch(ctx, name,
		types.MergePatchType, data, metav1.PatchOptions{}, "status")
	return err
}

// ---------- Helpers ----------

// hasSnapshot checks if the cocoon-vm-snapshots ConfigMap has an entry for the VM name.
func (c *controller) hasSnapshot(ctx context.Context, ns, vmName string) bool {
	cm, err := c.clientset.CoreV1().ConfigMaps(ns).Get(ctx, "cocoon-vm-snapshots", metav1.GetOptions{})
	if err != nil {
		return false
	}
	_, ok := cm.Data[vmName]
	return ok
}
