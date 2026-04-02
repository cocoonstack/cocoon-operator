package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// csGVR is the GroupVersionResource for CocoonSet CRDs.
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

	apiVersion    = "cocoon.cis/v1alpha1"
	kindCocoonSet = "CocoonSet"

	roleMain     = "main"
	roleSubAgent = "sub-agent"
	roleToolbox  = "toolbox"

	phaseSuspended = "Suspended"
	phaseScaling   = "Scaling"
	phaseRunning   = "Running"
)

// ---------- CocoonSet reconcile ----------

// classifiedPods holds agent and toolbox pods classified by role.
type classifiedPods struct {
	agents    map[int]*corev1.Pod
	toolboxes map[string]*corev1.Pod
}

// classifyPods groups owned pods into agent pods (by slot) and toolbox pods (by name).
func classifyPods(pods []corev1.Pod, csName string) classifiedPods {
	cp := classifiedPods{
		agents:    map[int]*corev1.Pod{},
		toolboxes: map[string]*corev1.Pod{},
	}
	prefix := len(csName) + 1
	for i := range pods {
		pod := &pods[i]
		switch pod.Labels[labelRole] {
		case roleMain, roleSubAgent:
			if slotStr, ok := pod.Labels[labelSlot]; ok {
				if slot, err := strconv.Atoi(slotStr); err == nil {
					cp.agents[slot] = pod
				}
			}
		case roleToolbox:
			if prefix > len(pod.Name) {
				continue
			}
			cp.toolboxes[pod.Name[prefix:]] = pod
		}
	}
	return cp
}

// reconcileCocoonSet handles CocoonSet events from the informer.
func (c *controller) reconcileCocoonSet(ctx context.Context, ns, name string) error {
	logger := log.WithFunc("controller.reconcileCocoonSet")

	cs, err := c.dynClient.Resource(csGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		logger.Errorf(ctx, err, "get failed %s/%s", ns, name)
		return err
	}

	spec := getMap(cs.Object, "spec")
	agentSpec := getMap(spec, "agent")
	suspend, _ := spec["suspend"].(bool)
	replicas := toInt64(agentSpec["replicas"])
	toolboxSpecs := getSlice(spec, "toolboxes")

	// List owned pods.
	ownedPods, err := c.listOwnedPods(ctx, ns, name)
	if err != nil {
		logger.Errorf(ctx, err, "list pods %s/%s", ns, name)
		return err
	}

	cp := classifyPods(ownedPods, name)
	desired := int(1 + replicas)

	// Handle suspend: hibernate all pods.
	if suspend {
		for i := range ownedPods {
			pod := &ownedPods[i]
			if pod.Annotations[annHibernate] != valTrue {
				c.patchPodAnnotation(ctx, ns, name, pod.Name, annHibernate, valTrue, "suspended")
			}
		}
		if err := c.updateCocoonSetStatus(ctx, ns, name, buildCocoonSetStatus(phaseSuspended, ownedPods, name, desired)); err != nil {
			logger.Errorf(ctx, err, "update suspended status %s/%s", ns, name)
		}
		return nil
	}

	// Not suspended: remove hibernate annotation from all pods.
	for i := range ownedPods {
		pod := &ownedPods[i]
		if pod.Annotations[annHibernate] == valTrue {
			c.patchPodAnnotation(ctx, ns, name, pod.Name, annHibernate, "", "unsuspended")
		}
	}

	// Ensure main agent (slot-0).
	mainPod, hasMain := cp.agents[0]
	if !hasMain {
		pod := buildAgentPod(ctx, cs, 0, "")
		if _, err := c.clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			logger.Errorf(ctx, err, "create main agent %s/%s", ns, name)
			return err
		}
		logger.Infof(ctx, "created main agent pod %s/%s %s", ns, name, pod.Name)
		if err := c.updateCocoonSetStatus(ctx, ns, name, buildCocoonSetStatus(phaseScaling, ownedPods, name, desired)); err != nil {
			logger.Errorf(ctx, err, "update scaling status %s/%s", ns, name)
		}
		return nil
	}

	if mainPod.Status.Phase != corev1.PodRunning {
		logger.Debugf(ctx, "main agent not ready %s/%s phase=%s", ns, name, mainPod.Status.Phase)
		if err := c.updateCocoonSetStatus(ctx, ns, name, buildCocoonSetStatus(phaseScaling, ownedPods, name, desired)); err != nil {
			logger.Errorf(ctx, err, "update scaling status %s/%s", ns, name)
		}
		return nil
	}

	// Scale sub-agents and toolboxes.
	c.scaleSubAgents(ctx, cs, ns, name, cp.agents, replicas)
	c.ensureToolboxes(ctx, cs, ns, name, cp.toolboxes, toolboxSpecs)

	// Update status -- re-list pods to get current state after creates/deletes.
	ownedPods, _ = c.listOwnedPods(ctx, ns, name)
	phase := phaseRunning
	readyCount := 0
	for i := range ownedPods {
		if ownedPods[i].Status.Phase == corev1.PodRunning {
			readyCount++
		}
	}
	if readyCount < desired+len(toolboxSpecs) {
		phase = phaseScaling
	}
	if err := c.updateCocoonSetStatus(ctx, ns, name, buildCocoonSetStatus(phase, ownedPods, name, desired)); err != nil {
		logger.Errorf(ctx, err, "update %s status %s/%s", phase, ns, name)
	}
	return nil
}

// scaleSubAgents creates missing sub-agents and deletes excess ones.
func (c *controller) scaleSubAgents(ctx context.Context, cs *unstructured.Unstructured, ns, name string, agentPods map[int]*corev1.Pod, replicas int64) {
	logger := log.WithFunc("controller.scaleSubAgents")
	mainVMName := agentPods[0].Annotations[annVMName]

	// Scale up: create missing sub-agents.
	for slot := 1; slot <= int(replicas); slot++ {
		if _, exists := agentPods[slot]; !exists {
			pod := buildAgentPod(ctx, cs, slot, mainVMName)
			if _, err := c.clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				logger.Errorf(ctx, err, "create sub-agent %s/%s slot %d", ns, name, slot)
			} else {
				logger.Infof(ctx, "created sub-agent pod %s/%s %s fork from %s", ns, name, pod.Name, mainVMName)
			}
		}
	}

	// Scale down: delete excess sub-agents (highest slot first).
	var excessSlots []int
	for slot := range agentPods {
		if slot > int(replicas) {
			excessSlots = append(excessSlots, slot)
		}
	}
	slices.SortFunc(excessSlots, func(a, b int) int { return cmp.Compare(b, a) })
	for _, slot := range excessSlots {
		pod := agentPods[slot]
		if err := c.clientset.CoreV1().Pods(ns).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
			logger.Errorf(ctx, err, "delete excess slot %s/%s %d", ns, name, slot)
		} else {
			logger.Infof(ctx, "deleted excess sub-agent pod %s/%s %s slot %d", ns, name, pod.Name, slot)
		}
	}
}

// ensureToolboxes creates missing toolbox pods.
func (c *controller) ensureToolboxes(ctx context.Context, cs *unstructured.Unstructured, ns, name string, toolboxPods map[string]*corev1.Pod, toolboxSpecs []any) {
	logger := log.WithFunc("controller.ensureToolboxes")
	for _, tbRaw := range toolboxSpecs {
		tb, ok := tbRaw.(map[string]any)
		if !ok {
			continue
		}
		tbName, _ := tb["name"].(string)
		if tbName == "" {
			continue
		}
		if _, exists := toolboxPods[tbName]; !exists {
			pod := buildToolboxPod(ctx, cs, tb)
			if _, err := c.clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				logger.Errorf(ctx, err, "create toolbox %s/%s %s", ns, name, tbName)
			} else {
				logger.Infof(ctx, "created toolbox pod %s/%s %s", ns, name, pod.Name)
			}
		}
	}
}

// ---------- Pod builders ----------

func managedPodAnnotations(vmName string, values map[string]string) map[string]string {
	annotations := map[string]string{
		annVMName: vmName,
	}
	for key, value := range values {
		if value == "" {
			continue
		}
		annotations[key] = value
	}
	return annotations
}

func managedPodLabels(csName, role string) map[string]string {
	return map[string]string{
		labelCocoonSet: csName,
		labelRole:      role,
		"app":          csName,
	}
}

func newManagedPod(cs *unstructured.Unstructured, podName, role, image, nodeName string, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            podName,
			Namespace:       cs.GetNamespace(),
			Labels:          managedPodLabels(cs.GetName(), role),
			Annotations:     annotations,
			OwnerReferences: ownerRef(cs),
		},
		Spec: corev1.PodSpec{
			NodeName:    nodeName,
			Tolerations: vkTolerations(),
			Containers:  []corev1.Container{{Name: "vm", Image: image}},
		},
	}
}

// buildAgentPod creates a Pod for an agent slot.
// If forkFrom is non-empty, it sets the fork-from annotation for sub-agents.
func buildAgentPod(ctx context.Context, cs *unstructured.Unstructured, slot int, forkFrom string) *corev1.Pod {
	name := cs.GetName()
	ns := cs.GetNamespace()
	spec := getMap(cs.Object, "spec")
	agentSpec := getMap(spec, "agent")
	nodeName := getTargetNodeName(spec)
	snapshotPolicy := stringDefault(spec, "snapshotPolicy", "always")

	image, _ := agentSpec["image"].(string)
	storage, _ := agentSpec["storage"].(string)

	podName := fmt.Sprintf("%s-%d", name, slot)
	vmName := fmt.Sprintf("vk-%s-%s-%d", ns, name, slot)

	role := roleMain
	if slot > 0 {
		role = roleSubAgent
	}

	annotations := managedPodAnnotations(vmName, map[string]string{
		annMode:           "clone",
		annImage:          image,
		annManaged:        valTrue,
		annOS:             "linux",
		annStorage:        storage,
		annSnapshotPolicy: snapshotPolicy,
	})
	if forkFrom != "" && slot > 0 {
		annotations[annForkFrom] = forkFrom
	}

	pod := newManagedPod(cs, podName, role, image, nodeName, annotations)
	pod.Labels[labelSlot] = strconv.Itoa(slot)

	applyResources(ctx, &pod.Spec.Containers[0], getMap(agentSpec, "resources"))
	applyEnvFrom(&pod.Spec.Containers[0], agentSpec)

	if sa, ok := agentSpec["serviceAccountName"].(string); ok && sa != "" {
		pod.Spec.ServiceAccountName = sa
	}

	return pod
}

// buildToolboxPod creates a Pod for a toolbox entry.
func buildToolboxPod(ctx context.Context, cs *unstructured.Unstructured, tb map[string]any) *corev1.Pod {
	csName := cs.GetName()
	ns := cs.GetNamespace()
	spec := getMap(cs.Object, "spec")
	nodeName := getTargetNodeName(spec)
	snapshotPolicy := stringDefault(spec, "snapshotPolicy", "always")

	tbName, _ := tb["name"].(string)
	tbOS := stringDefault(tb, "os", "linux")
	tbImage, _ := tb["image"].(string)
	tbMode := stringDefault(tb, "mode", "run")
	tbStorage, _ := tb["storage"].(string)
	statusHints := lookupToolboxRuntimeHints(cs, tbName)

	podName := fmt.Sprintf("%s-%s", csName, tbName)
	vmName := fmt.Sprintf("vk-%s-%s", ns, tbName)

	annotations := managedPodAnnotations(vmName, map[string]string{
		annMode:           tbMode,
		annManaged:        valTrue,
		annOS:             tbOS,
		annImage:          tbImage,
		annStorage:        tbStorage,
		annSnapshotPolicy: snapshotPolicy,
	})

	if tbMode == "static" {
		applyStaticHints(annotations, tb, statusHints)
	}

	pod := newManagedPod(cs, podName, roleToolbox, tbImage, nodeName, annotations)

	applyResources(ctx, &pod.Spec.Containers[0], getMap(tb, "resources"))

	return pod
}

// ---------- CocoonSet status ----------

// updateCocoonSetStatus patches the status subresource of a CocoonSet.
func (c *controller) updateCocoonSetStatus(ctx context.Context, ns, name string, status map[string]any) error {
	return c.patchStatus(ctx, csGVR, ns, name, status)
}

// podIP returns the pod's IP, preferring PodIP over the annotation fallback.
func podIP(pod *corev1.Pod) string {
	if ip := pod.Status.PodIP; ip != "" {
		return ip
	}
	return pod.Annotations[annIP]
}

// buildCocoonSetStatus builds a status map from current pod state.
func buildCocoonSetStatus(phase string, pods []corev1.Pod, csName string, desiredAgents int) map[string]any {
	var agents []any
	var toolboxes []any
	readyAgents := 0
	prefix := len(csName) + 1

	for i := range pods {
		pod := &pods[i]
		role := pod.Labels[labelRole]

		podPhase := string(pod.Status.Phase)
		if podPhase == "" {
			podPhase = "Pending"
		}

		switch role {
		case roleMain, roleSubAgent:
			slot := 0
			if s, ok := pod.Labels[labelSlot]; ok {
				slot, _ = strconv.Atoi(s)
			}
			if pod.Status.Phase == corev1.PodRunning {
				readyAgents++
			}
			agent := map[string]any{
				"slot":    int64(slot),
				"role":    role,
				"podName": pod.Name,
				"vmName":  pod.Annotations[annVMName],
				"phase":   podPhase,
			}
			if vmID, ok := pod.Annotations[annVMID]; ok {
				agent["vmID"] = vmID
			}
			if ip := podIP(pod); ip != "" {
				agent["ip"] = ip
			}
			if forkFrom, ok := pod.Annotations[annForkFrom]; ok && forkFrom != "" {
				agent["forkedFrom"] = forkFrom
			}
			agents = append(agents, agent)

		case roleToolbox:
			if prefix > len(pod.Name) {
				continue
			}
			tbName := pod.Name[prefix:]
			tb := map[string]any{
				"name":    tbName,
				"podName": pod.Name,
				"vmName":  pod.Annotations[annVMName],
				"phase":   podPhase,
			}
			if ip := podIP(pod); ip != "" {
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

	status := map[string]any{
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

// ---------- Helpers ----------

// patchPodAnnotation sets or removes an annotation on a pod.
// Pass a non-empty value to set, or an empty string to remove the annotation.
func (c *controller) patchPodAnnotation(ctx context.Context, ns, csName, podName, key, value, verb string) { //nolint:unparam // key is parameterized for reuse across annotation types
	logger := log.WithFunc("controller.patchPodAnnotation")
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				key: annotationPatchValue(value),
			},
		},
	})
	if err != nil {
		logger.Errorf(ctx, err, "cocoonset %s/%s: marshal patch for pod %s", ns, csName, podName)
		return
	}
	if _, err := c.clientset.CoreV1().Pods(ns).Patch(ctx, podName,
		types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		logger.Errorf(ctx, err, "cocoonset %s/%s: %s pod %s", ns, csName, verb, podName)
	} else {
		logger.Infof(ctx, "cocoonset %s/%s: %s pod %s", ns, csName, verb, podName)
	}
}

func annotationPatchValue(value string) any {
	if value == "" {
		return nil
	}
	return value
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

// ownerRef builds the standard OwnerReference slice for a CocoonSet-owned pod.
func ownerRef(cs *unstructured.Unstructured) []metav1.OwnerReference {
	blockOwnerDeletion := true
	isController := true
	return []metav1.OwnerReference{
		{
			APIVersion:         apiVersion,
			Kind:               kindCocoonSet,
			Name:               cs.GetName(),
			UID:                cs.GetUID(),
			Controller:         &isController,
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}
}

// vkTolerations returns the standard virtual-kubelet toleration.
func vkTolerations() []corev1.Toleration {
	return []corev1.Toleration{
		{
			Key:      "virtual-kubelet.io/provider",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}
}

// applyResources copies CPU/memory limits from an unstructured map to a container.
func applyResources(ctx context.Context, container *corev1.Container, resources map[string]any) {
	if cpu, ok := resources["cpu"].(string); ok {
		if container.Resources.Limits == nil {
			container.Resources.Limits = corev1.ResourceList{}
		}
		container.Resources.Limits[corev1.ResourceCPU] = mustParseQuantity(ctx, cpu)
	}
	if mem, ok := resources["memory"].(string); ok {
		if container.Resources.Limits == nil {
			container.Resources.Limits = corev1.ResourceList{}
		}
		container.Resources.Limits[corev1.ResourceMemory] = mustParseQuantity(ctx, mem)
	}
}

// applyEnvFrom copies envFrom sources from an unstructured agent spec to a container.
func applyEnvFrom(container *corev1.Container, agentSpec map[string]any) {
	envFromRaw, ok := agentSpec["envFrom"]
	if !ok {
		return
	}
	envFromSlice, ok := envFromRaw.([]any)
	if !ok {
		return
	}
	for _, item := range envFromSlice {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if cmRef := getMap(m, "configMapRef"); len(cmRef) > 0 {
			if cmName, _ := cmRef["name"].(string); cmName != "" {
				container.EnvFrom = append(container.EnvFrom,
					corev1.EnvFromSource{
						ConfigMapRef: &corev1.ConfigMapEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
						},
					})
			}
		}
		if secretRef := getMap(m, "secretRef"); len(secretRef) > 0 {
			if secretName, _ := secretRef["name"].(string); secretName != "" {
				container.EnvFrom = append(container.EnvFrom,
					corev1.EnvFromSource{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						},
					})
			}
		}
	}
}

// applyStaticHints sets IP, VMID, and VNC port annotations for static toolboxes,
// preferring runtime status hints over spec hints.
func applyStaticHints(annotations map[string]string, tb, statusHints map[string]any) {
	if ip := stringDefault(statusHints, "ip", ""); ip != "" {
		annotations[annIP] = ip
	} else if ip, ok := tb["staticIP"].(string); ok && ip != "" {
		annotations[annIP] = ip
	}
	if vmID := stringDefault(statusHints, "vmID", ""); vmID != "" {
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

func getTargetNodeName(spec map[string]any) string {
	if nodeName, _ := spec["nodeName"].(string); nodeName != "" {
		return nodeName
	}
	return "cocoon-pool"
}

func lookupToolboxRuntimeHints(cs *unstructured.Unstructured, tbName string) map[string]any {
	status := getMap(cs.Object, "status")
	for _, raw := range getSlice(status, "toolboxes") {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if name, _ := m["name"].(string); name == tbName {
			return m
		}
	}
	return nil
}
