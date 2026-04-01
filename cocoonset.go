package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

var csGVR = schema.GroupVersionResource{
	Group:    "cocoon.cis",
	Version:  "v1alpha1",
	Resource: "cocoonsets",
}

const (
	labelCocoonSet = "cocoon.cis/cocoonset"
	labelRole      = "cocoon.cis/role"
	labelSlot      = "cocoon.cis/slot"

	annMode           = "cocoon.cis/mode"
	annImage          = "cocoon.cis/image"
	annStorage        = "cocoon.cis/storage"
	annManaged        = "cocoon.cis/managed"
	annOS             = "cocoon.cis/os"
	annForkFrom       = "cocoon.cis/fork-from"
	annSnapshotPolicy = "cocoon.cis/snapshot-policy"
	annIP             = "cocoon.cis/ip"
	annVMID           = "cocoon.cis/vm-id"
	annVNCPort        = "cocoon.cis/vnc-port"
)

// reconcileCocoonSet handles CocoonSet events from the informer.
func (c *controller) reconcileCocoonSet(ctx context.Context, ns, name string) error {
	// 1. Get CocoonSet
	cs, err := c.dynClient.Resource(csGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("cocoonset %s/%s: get failed: %v", ns, name, err)
		return err
	}

	spec := getMap(cs.Object, "spec")
	agentSpec := getMap(spec, "agent")
	suspend, _ := spec["suspend"].(bool)
	snapshotPolicy, _ := spec["snapshotPolicy"].(string)
	if snapshotPolicy == "" {
		snapshotPolicy = "always"
	}

	image, _ := agentSpec["image"].(string)
	replicas := int64(0)
	if r, ok := agentSpec["replicas"]; ok {
		switch v := r.(type) {
		case int64:
			replicas = v
		case float64:
			replicas = int64(v)
		}
	}

	toolboxSpecs := getSlice(spec, "toolboxes")

	// 2. List owned pods
	ownedPods, err := c.listOwnedPods(ctx, ns, name)
	if err != nil {
		klog.Errorf("cocoonset %s/%s: list pods: %v", ns, name, err)
		return err
	}

	// 3. Classify pods
	agentPods := map[int]*corev1.Pod{}      // slot -> pod
	toolboxPods := map[string]*corev1.Pod{} // toolbox name -> pod
	for i := range ownedPods {
		pod := &ownedPods[i]
		role := pod.Labels[labelRole]
		switch role {
		case "main", "sub-agent":
			if slotStr, ok := pod.Labels[labelSlot]; ok {
				if slot, err := strconv.Atoi(slotStr); err == nil {
					agentPods[slot] = pod
				}
			}
		case "toolbox":
			// Derive toolbox name from pod name: {cocoonset}-{toolboxName}
			tbName := pod.Name[len(name)+1:]
			toolboxPods[tbName] = pod
		}
	}

	// 4. Handle suspend
	if suspend {
		for i := range ownedPods {
			pod := &ownedPods[i]
			if pod.Annotations[annHibernate] != "true" {
				patch := fmt.Sprintf(`{"metadata":{"annotations":{"%s":"true"}}}`, annHibernate)
				if _, err := c.clientset.CoreV1().Pods(ns).Patch(ctx, pod.Name,
					types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
					klog.Errorf("cocoonset %s/%s: suspend pod %s: %v", ns, name, pod.Name, err)
				} else {
					klog.Infof("cocoonset %s/%s: suspended pod %s", ns, name, pod.Name)
				}
			}
		}
		c.updateCocoonSetStatus(ctx, ns, name, buildCocoonSetStatus("Suspended", ownedPods, name, int(1+replicas)))
		return nil
	}

	// Remove hibernate annotation if suspend=false
	for i := range ownedPods {
		pod := &ownedPods[i]
		if pod.Annotations[annHibernate] == "true" {
			patch := fmt.Sprintf(`{"metadata":{"annotations":{"%s":null}}}`, annHibernate)
			if _, err := c.clientset.CoreV1().Pods(ns).Patch(ctx, pod.Name,
				types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
				klog.Errorf("cocoonset %s/%s: unsuspend pod %s: %v", ns, name, pod.Name, err)
			} else {
				klog.Infof("cocoonset %s/%s: unsuspended pod %s", ns, name, pod.Name)
			}
		}
	}

	// 5. Ensure main agent (slot-0)
	mainPod, hasMain := agentPods[0]
	if !hasMain {
		pod := buildAgentPod(cs, 0, "")
		if _, err := c.clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			klog.Errorf("cocoonset %s/%s: create main agent: %v", ns, name, err)
			return err
		}
		klog.Infof("cocoonset %s/%s: created main agent pod %s", ns, name, pod.Name)
		c.updateCocoonSetStatus(ctx, ns, name, buildCocoonSetStatus("Scaling", ownedPods, name, int(1+replicas)))
		return nil // requeue via resync
	}

	// Check if main is ready
	if mainPod.Status.Phase != corev1.PodRunning {
		klog.V(2).Infof("cocoonset %s/%s: main agent not ready (phase=%s), waiting", ns, name, mainPod.Status.Phase)
		c.updateCocoonSetStatus(ctx, ns, name, buildCocoonSetStatus("Scaling", ownedPods, name, int(1+replicas)))
		return nil // requeue via resync
	}

	// 6. Scale sub-agents (slot 1..replicas)
	mainVMName := mainPod.Annotations[annVMName]

	for slot := 1; slot <= int(replicas); slot++ {
		if _, exists := agentPods[slot]; !exists {
			pod := buildAgentPod(cs, slot, mainVMName)
			if _, err := c.clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				klog.Errorf("cocoonset %s/%s: create sub-agent slot %d: %v", ns, name, slot, err)
			} else {
				klog.Infof("cocoonset %s/%s: created sub-agent pod %s (fork from %s)", ns, name, pod.Name, mainVMName)
			}
		}
	}

	// Scale down: delete excess sub-agents (highest slot first)
	var excessSlots []int
	for slot := range agentPods {
		if slot > int(replicas) {
			excessSlots = append(excessSlots, slot)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(excessSlots)))
	for _, slot := range excessSlots {
		pod := agentPods[slot]
		if err := c.clientset.CoreV1().Pods(ns).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
			klog.Errorf("cocoonset %s/%s: delete excess slot %d: %v", ns, name, slot, err)
		} else {
			klog.Infof("cocoonset %s/%s: deleted excess sub-agent pod %s (slot %d)", ns, name, pod.Name, slot)
		}
	}

	// 7. Ensure toolboxes
	for _, tbRaw := range toolboxSpecs {
		tb, ok := tbRaw.(map[string]interface{})
		if !ok {
			continue
		}
		tbName, _ := tb["name"].(string)
		if tbName == "" {
			continue
		}
		if _, exists := toolboxPods[tbName]; !exists {
			pod := buildToolboxPod(cs, tb)
			if _, err := c.clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				klog.Errorf("cocoonset %s/%s: create toolbox %s: %v", ns, name, tbName, err)
			} else {
				klog.Infof("cocoonset %s/%s: created toolbox pod %s", ns, name, pod.Name)
			}
		}
	}

	// 8. Update status
	_ = image
	_ = snapshotPolicy
	// Re-list pods to get current state after possible creates/deletes
	ownedPods, _ = c.listOwnedPods(ctx, ns, name)
	phase := "Running"
	readyCount := 0
	for i := range ownedPods {
		if ownedPods[i].Status.Phase == corev1.PodRunning {
			readyCount++
		}
	}
	desired := int(1 + replicas)
	if readyCount < desired+len(toolboxSpecs) {
		phase = "Scaling"
	}
	c.updateCocoonSetStatus(ctx, ns, name, buildCocoonSetStatus(phase, ownedPods, name, desired))
	return nil
}

// buildAgentPod creates a Pod for an agent slot.
// If forkFrom is non-empty, it sets the fork-from annotation for sub-agents.
func buildAgentPod(cs *unstructured.Unstructured, slot int, forkFrom string) *corev1.Pod {
	name := cs.GetName()
	ns := cs.GetNamespace()
	spec := getMap(cs.Object, "spec")
	agentSpec := getMap(spec, "agent")
	targetNodeName := getTargetNodeName(spec)
	snapshotPolicy, _ := spec["snapshotPolicy"].(string)
	if snapshotPolicy == "" {
		snapshotPolicy = "always"
	}

	image, _ := agentSpec["image"].(string)
	storage, _ := agentSpec["storage"].(string)

	podName := fmt.Sprintf("%s-%d", name, slot)
	vmName := fmt.Sprintf("vk-%s-%s-%d", ns, name, slot)

	role := "main"
	mode := "clone"
	if slot > 0 {
		role = "sub-agent"
	}

	annotations := map[string]string{
		annVMName:         vmName,
		annMode:           mode,
		annImage:          image,
		annManaged:        "true",
		annOS:             "linux",
		annSnapshotPolicy: snapshotPolicy,
	}
	if storage != "" {
		annotations[annStorage] = storage
	}
	if forkFrom != "" && slot > 0 {
		annotations[annForkFrom] = forkFrom
	}

	labels := map[string]string{
		labelCocoonSet: name,
		labelRole:      role,
		labelSlot:      strconv.Itoa(slot),
		"app":          name,
	}

	uid := cs.GetUID()
	apiVersion := "cocoon.cis/v1alpha1"
	kind := "CocoonSet"
	blockOwnerDeletion := true
	isController := true

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   ns,
			Labels:      labels,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         apiVersion,
					Kind:               kind,
					Name:               name,
					UID:                uid,
					Controller:         &isController,
					BlockOwnerDeletion: &blockOwnerDeletion,
				},
			},
		},
		Spec: corev1.PodSpec{
			NodeName: targetNodeName,
			Tolerations: []corev1.Toleration{
				{
					Key:      "virtual-kubelet.io/provider",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "vm",
					Image: image,
				},
			},
		},
	}

	// Copy resources to container
	resources := getMap(agentSpec, "resources")
	if cpu, ok := resources["cpu"].(string); ok {
		if pod.Spec.Containers[0].Resources.Limits == nil {
			pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
		}
		pod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = mustParseQuantity(cpu)
	}
	if mem, ok := resources["memory"].(string); ok {
		if pod.Spec.Containers[0].Resources.Limits == nil {
			pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
		}
		pod.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = mustParseQuantity(mem)
	}

	// Copy envFrom from spec.agent
	if envFromRaw, ok := agentSpec["envFrom"]; ok {
		if envFromSlice, ok := envFromRaw.([]interface{}); ok {
			for _, item := range envFromSlice {
				if m, ok := item.(map[string]interface{}); ok {
					if cmRef := getMap(m, "configMapRef"); len(cmRef) > 0 {
						cmName, _ := cmRef["name"].(string)
						if cmName != "" {
							pod.Spec.Containers[0].EnvFrom = append(pod.Spec.Containers[0].EnvFrom,
								corev1.EnvFromSource{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
									},
								})
						}
					}
					if secretRef := getMap(m, "secretRef"); len(secretRef) > 0 {
						secretName, _ := secretRef["name"].(string)
						if secretName != "" {
							pod.Spec.Containers[0].EnvFrom = append(pod.Spec.Containers[0].EnvFrom,
								corev1.EnvFromSource{
									SecretRef: &corev1.SecretEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									},
								})
						}
					}
				}
			}
		}
	}

	// Copy serviceAccountName
	if sa, ok := agentSpec["serviceAccountName"].(string); ok && sa != "" {
		pod.Spec.ServiceAccountName = sa
	}

	return pod
}

// buildToolboxPod creates a Pod for a toolbox entry.
func buildToolboxPod(cs *unstructured.Unstructured, tb map[string]interface{}) *corev1.Pod {
	csName := cs.GetName()
	ns := cs.GetNamespace()
	spec := getMap(cs.Object, "spec")
	targetNodeName := getTargetNodeName(spec)
	snapshotPolicy, _ := spec["snapshotPolicy"].(string)
	if snapshotPolicy == "" {
		snapshotPolicy = "always"
	}

	tbName, _ := tb["name"].(string)
	tbOS, _ := tb["os"].(string)
	if tbOS == "" {
		tbOS = "linux"
	}
	tbImage, _ := tb["image"].(string)
	tbMode, _ := tb["mode"].(string)
	if tbMode == "" {
		tbMode = "run"
	}
	tbStorage, _ := tb["storage"].(string)
	statusHints := lookupToolboxRuntimeHints(cs, tbName)

	podName := fmt.Sprintf("%s-%s", csName, tbName)
	vmName := fmt.Sprintf("vk-%s-%s", ns, tbName)

	annotations := map[string]string{
		annVMName:         vmName,
		annMode:           tbMode,
		annManaged:        "true",
		annOS:             tbOS,
		annSnapshotPolicy: snapshotPolicy,
	}
	if tbImage != "" {
		annotations[annImage] = tbImage
	}
	if tbStorage != "" {
		annotations[annStorage] = tbStorage
	}

	if tbMode == "static" {
		// Static toolbox: prefer the last runtime metadata persisted in status so
		// pod recreation after controller/provider restart doesn't regress to a
		// stale spec hint.
		if ip := getStringValue(statusHints, "ip"); ip != "" {
			annotations[annIP] = ip
		} else if ip, ok := tb["staticIP"].(string); ok && ip != "" {
			annotations[annIP] = ip
		}
		if vmID := getStringValue(statusHints, "vmID"); vmID != "" {
			annotations[annVMID] = vmID
		} else if vmID, ok := tb["staticVMID"].(string); ok && vmID != "" {
			annotations[annVMID] = vmID
		}
		if vncPort, ok := getInt64Value(statusHints, "vncPort"); ok {
			annotations[annVNCPort] = strconv.FormatInt(vncPort, 10)
		} else if vncPort, ok := tb["vncPort"]; ok {
			switch v := vncPort.(type) {
			case int64:
				annotations[annVNCPort] = strconv.FormatInt(v, 10)
			case float64:
				annotations[annVNCPort] = strconv.FormatInt(int64(v), 10)
			}
		}
	}

	labels := map[string]string{
		labelCocoonSet: csName,
		labelRole:      "toolbox",
		"app":          csName,
	}

	uid := cs.GetUID()
	apiVersion := "cocoon.cis/v1alpha1"
	kind := "CocoonSet"
	blockOwnerDeletion := true
	isController := true

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   ns,
			Labels:      labels,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         apiVersion,
					Kind:               kind,
					Name:               csName,
					UID:                uid,
					Controller:         &isController,
					BlockOwnerDeletion: &blockOwnerDeletion,
				},
			},
		},
		Spec: corev1.PodSpec{
			NodeName: targetNodeName,
			Tolerations: []corev1.Toleration{
				{
					Key:      "virtual-kubelet.io/provider",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "vm",
					Image: tbImage,
				},
			},
		},
	}

	// Copy resources
	resources := getMap(tb, "resources")
	if cpu, ok := resources["cpu"].(string); ok {
		if pod.Spec.Containers[0].Resources.Limits == nil {
			pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
		}
		pod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = mustParseQuantity(cpu)
	}
	if mem, ok := resources["memory"].(string); ok {
		if pod.Spec.Containers[0].Resources.Limits == nil {
			pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
		}
		pod.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = mustParseQuantity(mem)
	}

	return pod
}

func toolboxConnType(osType string, hasVNCPort bool) string {
	switch {
	case hasVNCPort:
		return "vnc"
	case osType == "android":
		return "adb"
	case osType == "windows":
		return "rdp"
	default:
		return "ssh"
	}
}

func getTargetNodeName(spec map[string]interface{}) string {
	if nodeName, _ := spec["nodeName"].(string); nodeName != "" {
		return nodeName
	}
	return "cocoon-pool"
}

func lookupToolboxRuntimeHints(cs *unstructured.Unstructured, tbName string) map[string]interface{} {
	status := getMap(cs.Object, "status")
	for _, raw := range getSlice(status, "toolboxes") {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if name, _ := m["name"].(string); name == tbName {
			return m
		}
	}
	return nil
}

func getStringValue(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getInt64Value(m map[string]interface{}, key string) (int64, bool) {
	if m == nil {
		return 0, false
	}
	switch v := m[key].(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

// listOwnedPods returns pods with the cocoon.cis/cocoonset label matching the given name.
func (c *controller) listOwnedPods(ctx context.Context, ns, csName string) ([]corev1.Pod, error) {
	pods, err := c.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", labelCocoonSet, csName),
	})
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// updateCocoonSetStatus patches the status subresource of a CocoonSet.
func (c *controller) updateCocoonSetStatus(ctx context.Context, ns, name string, status map[string]interface{}) error {
	patch := map[string]interface{}{
		"status": status,
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	_, err = c.dynClient.Resource(csGVR).Namespace(ns).Patch(ctx, name,
		types.MergePatchType, data, metav1.PatchOptions{}, "status")
	if err != nil {
		klog.Errorf("cocoonset %s/%s: update status: %v", ns, name, err)
	}
	return err
}

// buildCocoonSetStatus builds a status map from current pod state.
func buildCocoonSetStatus(phase string, pods []corev1.Pod, csName string, desiredAgents int) map[string]interface{} {
	var agents []interface{}
	var toolboxes []interface{}
	readyAgents := 0

	for i := range pods {
		pod := &pods[i]
		role := pod.Labels[labelRole]

		podPhase := string(pod.Status.Phase)
		if podPhase == "" {
			podPhase = "Pending"
		}

		switch role {
		case "main", "sub-agent":
			slot := 0
			if s, ok := pod.Labels[labelSlot]; ok {
				slot, _ = strconv.Atoi(s)
			}
			if pod.Status.Phase == corev1.PodRunning {
				readyAgents++
			}
			agent := map[string]interface{}{
				"slot":    int64(slot),
				"role":    role,
				"podName": pod.Name,
				"vmName":  pod.Annotations[annVMName],
				"phase":   podPhase,
			}
			if vmID, ok := pod.Annotations[annVMID]; ok {
				agent["vmID"] = vmID
			}
			if ip := pod.Status.PodIP; ip != "" {
				agent["ip"] = ip
			} else if ip, ok := pod.Annotations[annIP]; ok {
				agent["ip"] = ip
			}
			if forkFrom, ok := pod.Annotations[annForkFrom]; ok && forkFrom != "" {
				agent["forkedFrom"] = forkFrom
			}
			agents = append(agents, agent)
		case "toolbox":
			tbName := pod.Name[len(csName)+1:]
			tb := map[string]interface{}{
				"name":    tbName,
				"podName": pod.Name,
				"vmName":  pod.Annotations[annVMName],
				"phase":   podPhase,
			}
			if ip := pod.Status.PodIP; ip != "" {
				tb["ip"] = ip
			} else if ip, ok := pod.Annotations[annIP]; ok {
				tb["ip"] = ip
			}
			if vmID, ok := pod.Annotations[annVMID]; ok && vmID != "" {
				tb["vmID"] = vmID
			}
			_, hasVNCPort := pod.Annotations[annVNCPort]
			tb["connType"] = toolboxConnType(pod.Annotations[annOS], hasVNCPort)
			if hasVNCPort {
				if port, err := strconv.Atoi(pod.Annotations[annVNCPort]); err == nil {
					tb["vncPort"] = int64(port)
				}
			}
			toolboxes = append(toolboxes, tb)
		}
	}

	status := map[string]interface{}{
		"phase":         phase,
		"readyAgents":   int64(readyAgents),
		"desiredAgents": int64(desiredAgents),
	}
	if agents != nil {
		status["agents"] = agents
	}
	if toolboxes != nil {
		status["toolboxes"] = toolboxes
	}
	return status
}

// getSlice extracts a []interface{} from a nested map.
func getSlice(obj map[string]interface{}, key string) []interface{} {
	if v, ok := obj[key]; ok {
		if s, ok := v.([]interface{}); ok {
			return s
		}
	}
	return nil
}

// mustParseQuantity parses a resource quantity string, returning a zero quantity on error.
func mustParseQuantity(s string) resource.Quantity {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		klog.Warningf("invalid quantity %q: %v", s, err)
		return resource.Quantity{}
	}
	return q
}
