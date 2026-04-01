// cocoon-operator — Hibernation controller for Cocoon VM pods.
//
// Watches Hibernation CRDs and drives pod hibernate/wake by annotating pods
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
//	# Install CRD
//	kubectl apply -f deploy/crd.yaml
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
	"os"
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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

var hibGVR = schema.GroupVersionResource{
	Group:    "cocoon.cis",
	Version:  "v1alpha1",
	Resource: "hibernations",
}

const (
	annHibernate = "cocoon.cis/hibernate"
	annVMName    = "cocoon.cis/vm-name"
)

func main() {
	klog.InitFlags(nil)

	config, err := buildConfig()
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

	// Watch Hibernation CRDs via dynamic informer.
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, 30*time.Second)
	hibInformer := factory.ForResource(hibGVR).Informer()

	hibInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { ctrl.reconcile(ctx, obj) },
		UpdateFunc: func(_, obj interface{}) { ctrl.reconcile(ctx, obj) },
	})

	// Watch CocoonSet CRDs via dynamic informer.
	csInformer := factory.ForResource(csGVR).Informer()

	csInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
			ctrl.reconcileCocoonSet(ctx, u.GetNamespace(), u.GetName())
		},
		UpdateFunc: func(_, obj interface{}) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
			ctrl.reconcileCocoonSet(ctx, u.GetNamespace(), u.GetName())
		},
	})

	// Watch Pods — if a pod's ownerRef points to a CocoonSet, reconcile that CocoonSet.
	podInformer := factory.ForResource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "pods",
	}).Informer()

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, obj interface{}) {
			ctrl.handlePodEvent(ctx, obj)
		},
		DeleteFunc: func(obj interface{}) {
			ctrl.handlePodEvent(ctx, obj)
		},
	})

	// Periodic re-sync to detect vk-cocoon status changes on pods.
	go func() {
		ticker := time.NewTicker(15 * time.Second)
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

// ---------- Controller ----------

type controller struct {
	clientset *kubernetes.Clientset
	dynClient dynamic.Interface
}

func (c *controller) reconcile(ctx context.Context, obj interface{}) {
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
		return // done
	}

	pod, err := c.clientset.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		c.updateStatus(ctx, ns, hibName, "Failed", fmt.Sprintf("pod not found: %v", err), "", "")
		return
	}

	vmName := pod.Annotations[annVMName]

	// Check if pod is already hibernated. We check the cocoon-vm-snapshots
	// ConfigMap — if vk-cocoon saved a snapshot for this VM name, hibernation
	// is complete. (Pod container status may not reflect hibernation due to VK
	// framework limitations.)
	if vmName != "" && c.hasSnapshot(ctx, ns, vmName) {
		c.updateStatus(ctx, ns, hibName, "Hibernated", "VM suspended to epoch", vmName, "")
		return
	}

	// Pod is still running — annotate to trigger vk-cocoon hibernate.
	if pod.Annotations[annHibernate] != "true" {
		patch := fmt.Sprintf(`{"metadata":{"annotations":{"%s":"true"}}}`, annHibernate)
		if _, err := c.clientset.CoreV1().Pods(ns).Patch(ctx, podName,
			types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
			klog.Errorf("hibernate %s/%s: patch pod: %v", ns, podName, err)
			c.updateStatus(ctx, ns, hibName, "Failed", fmt.Sprintf("patch failed: %v", err), vmName, "")
			return
		}
		klog.Infof("hibernate %s/%s: annotated pod, waiting for vk-cocoon", ns, podName)
	}

	if phase != "Hibernating" {
		c.updateStatus(ctx, ns, hibName, "Hibernating", "Waiting for vk-cocoon to snapshot VM", vmName, "")
	}
}

func (c *controller) reconcileWake(ctx context.Context, ns, hibName, podName, phase string) {
	if phase == "Active" {
		return // done
	}

	pod, err := c.clientset.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		c.updateStatus(ctx, ns, hibName, "Failed", fmt.Sprintf("pod not found: %v", err), "", "")
		return
	}

	vmName := pod.Annotations[annVMName]

	// Check if pod is running. We check that the hibernate annotation is gone
	// AND the VM snapshot has been consumed (cleared from ConfigMap).
	if pod.Annotations[annHibernate] != "true" && vmName != "" && !c.hasSnapshot(ctx, ns, vmName) {
		c.updateStatus(ctx, ns, hibName, "Active", "VM restored and running", vmName, "")
		return
	}

	// Remove hibernate annotation to trigger vk-cocoon wake.
	if pod.Annotations[annHibernate] == "true" {
		patch := fmt.Sprintf(`{"metadata":{"annotations":{"%s":null}}}`, annHibernate)
		if _, err := c.clientset.CoreV1().Pods(ns).Patch(ctx, podName,
			types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
			klog.Errorf("wake %s/%s: patch pod: %v", ns, podName, err)
			c.updateStatus(ctx, ns, hibName, "Failed", fmt.Sprintf("patch failed: %v", err), vmName, "")
			return
		}
		klog.Infof("wake %s/%s: removed hibernate annotation, waiting for vk-cocoon", ns, podName)
	}

	if phase != "Waking" {
		c.updateStatus(ctx, ns, hibName, "Waking", "Waiting for vk-cocoon to restore VM", vmName, "")
	}
}

// resync checks all Hibernation CRDs for status transitions (e.g. vk-cocoon
// finished hibernating/waking a VM since the last reconcile).
func (c *controller) resync(ctx context.Context, store cache.Store) {
	for _, obj := range store.List() {
		c.reconcile(ctx, obj)
	}
}

// handlePodEvent checks if a pod is owned by a CocoonSet and reconciles it.
func (c *controller) handlePodEvent(ctx context.Context, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		// Handle DeletedFinalStateUnknown (tombstone)
		if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			u, ok = d.Obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
		} else {
			return
		}
	}

	ownerRefs := u.GetOwnerReferences()
	for _, ref := range ownerRefs {
		if ref.Kind == "CocoonSet" && ref.APIVersion == "cocoon.cis/v1alpha1" {
			c.reconcileCocoonSet(ctx, u.GetNamespace(), ref.Name)
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
		c.reconcileCocoonSet(ctx, u.GetNamespace(), u.GetName())
	}
}

// ---------- Status update ----------

func (c *controller) updateStatus(ctx context.Context, ns, name, phase, message, vmName, snapshotRef string) {
	status := map[string]interface{}{
		"phase":              phase,
		"message":            message,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}
	if vmName != "" {
		status["vmName"] = vmName
	}
	if snapshotRef != "" {
		status["snapshotRef"] = snapshotRef
	}

	patch := map[string]interface{}{
		"status": status,
	}
	data, _ := json.Marshal(patch)

	_, err := c.dynClient.Resource(hibGVR).Namespace(ns).Patch(ctx, name,
		types.MergePatchType, data, metav1.PatchOptions{}, "status")
	if err != nil {
		klog.Errorf("updateStatus %s/%s → %s: %v", ns, name, phase, err)
	}
}

// ---------- Helpers ----------

func buildConfig() (*rest.Config, error) {
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if home := os.Getenv("HOME"); home != "" {
		if _, err := os.Stat(home + "/.kube/config"); err == nil {
			return clientcmd.BuildConfigFromFlags("", home+"/.kube/config")
		}
	}
	return rest.InClusterConfig()
}

func getMap(obj map[string]interface{}, key string) map[string]interface{} {
	if v, ok := obj[key]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			return m
		}
	}
	return map[string]interface{}{}
}

// hasSnapshot checks if the cocoon-vm-snapshots ConfigMap has an entry for the VM name.
func (c *controller) hasSnapshot(ctx context.Context, ns, vmName string) bool {
	cm, err := c.clientset.CoreV1().ConfigMaps(ns).Get(ctx, "cocoon-vm-snapshots", metav1.GetOptions{})
	if err != nil {
		return false
	}
	_, ok := cm.Data[vmName]
	return ok
}
