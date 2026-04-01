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

	"github.com/projecteru2/core/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/cocoonstack/cocoon-operator/k8sutil"
	"github.com/cocoonstack/cocoon-operator/logutil"
	"github.com/cocoonstack/cocoon-operator/version"
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

	phaseHibernated  = "Hibernated"
	phaseHibernating = "Hibernating"
	phaseWaking      = "Waking"
	phaseActive      = "Active"
	phaseFailed      = "Failed"
)

// controller holds the Kubernetes clients used by all reconcile loops.
type controller struct {
	clientset *kubernetes.Clientset
	dynClient dynamic.Interface
}

func main() {
	ctx := context.Background()

	logutil.Setup(ctx, "LOG_LEVEL")

	logger := log.WithFunc("main")

	config, err := k8sutil.LoadConfig()
	if err != nil {
		logger.Fatalf(ctx, err, "k8s config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Fatalf(ctx, err, "clientset: %v", err)
	}
	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		logger.Fatalf(ctx, err, "dynamic client: %v", err)
	}

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
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
	handleCS := func(obj any) {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return
		}
		if err := ctrl.reconcileCocoonSet(ctx, u.GetNamespace(), u.GetName()); err != nil {
			logger.Errorf(ctx, err, "cocoonset %s/%s: reconcile", u.GetNamespace(), u.GetName())
		}
	}
	csInformer := factory.ForResource(csGVR).Informer()
	_, _ = csInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint:errcheck // registration handle unused
		AddFunc:    handleCS,
		UpdateFunc: func(_, obj any) { handleCS(obj) },
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
	logger.Infof(ctx, "cocoon-operator %s started (rev=%s built=%s)", version.VERSION, version.REVISION, version.BUILTAT)

	<-ctx.Done()
	logger.Info(ctx, "cocoon-operator shutting down")
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
	if phase == phaseHibernated {
		return
	}

	pod, err := c.clientset.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		c.updateStatus(ctx, ns, hibName, phaseFailed, fmt.Sprintf("pod not found: %v", err), "")
		return
	}

	vmName := pod.Annotations[annVMName]

	// If vk-cocoon already saved a snapshot for this VM, hibernation is complete.
	if vmName != "" && c.hasSnapshot(ctx, ns, vmName) {
		c.updateStatus(ctx, ns, hibName, phaseHibernated, "VM suspended to epoch", vmName)
		return
	}

	// Annotate pod to trigger vk-cocoon hibernate.
	if pod.Annotations[annHibernate] != valTrue {
		c.patchPodAnnotation(ctx, ns, hibName, podName, annHibernate, valTrue, "hibernate")
	}

	if phase != phaseHibernating {
		c.updateStatus(ctx, ns, hibName, phaseHibernating, "Waiting for vk-cocoon to snapshot VM", vmName)
	}
}

func (c *controller) reconcileWake(ctx context.Context, ns, hibName, podName, phase string) {
	if phase == phaseActive {
		return
	}

	pod, err := c.clientset.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		c.updateStatus(ctx, ns, hibName, phaseFailed, fmt.Sprintf("pod not found: %v", err), "")
		return
	}

	vmName := pod.Annotations[annVMName]

	// Pod is awake when the hibernate annotation is gone and the snapshot is consumed.
	if pod.Annotations[annHibernate] != valTrue && vmName != "" && !c.hasSnapshot(ctx, ns, vmName) {
		c.updateStatus(ctx, ns, hibName, phaseActive, "VM restored and running", vmName)
		return
	}

	// Remove hibernate annotation to trigger vk-cocoon wake.
	if pod.Annotations[annHibernate] == valTrue {
		c.patchPodAnnotation(ctx, ns, hibName, podName, annHibernate, "", "wake")
	}

	if phase != phaseWaking {
		c.updateStatus(ctx, ns, hibName, phaseWaking, "Waiting for vk-cocoon to restore VM", vmName)
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
		if ref.Kind == kindCocoonSet && ref.APIVersion == apiVersion {
			if err := c.reconcileCocoonSet(ctx, u.GetNamespace(), ref.Name); err != nil {
				log.WithFunc("controller.handlePodEvent").Errorf(ctx, err, "cocoonset %s/%s: reconcile on pod event", u.GetNamespace(), ref.Name)
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
			log.WithFunc("controller.resyncCocoonSets").Errorf(ctx, err, "cocoonset %s/%s: reconcile on resync", u.GetNamespace(), u.GetName())
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
		log.WithFunc("controller.updateStatus").Errorf(ctx, err, "update status %s/%s -> %s", ns, name, phase)
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
