package main

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

// csGVR is the GroupVersionResource for CocoonSet CRDs.
var csGVR = schema.GroupVersionResource{
	Group:    "cocoon.cis",
	Version:  "v1alpha1",
	Resource: "cocoonsets",
}

const (
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
		switch pod.Labels[meta.LabelRole] {
		case meta.RoleMain, meta.RoleSubAgent:
			if slotStr, ok := pod.Labels[meta.LabelSlot]; ok {
				if slot, err := strconv.Atoi(slotStr); err == nil {
					cp.agents[slot] = pod
				}
			}
		case meta.RoleToolbox:
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

	rawCS, err := c.dynClient.Resource(csGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		logger.Errorf(ctx, err, "get failed %s/%s", ns, name)
		return err
	}

	cs, err := commonk8s.DecodeUnstructured[cocoonSet](rawCS)
	if err != nil {
		logger.Errorf(ctx, err, "decode failed %s/%s", ns, name)
		return err
	}

	suspend := cs.Spec.Suspend
	replicas := cs.Spec.Agent.Replicas
	toolboxSpecs := cs.Spec.Toolboxes

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
			if pod.Annotations[meta.AnnotationHibernate] != valTrue {
				c.patchPodAnnotation(ctx, ns, name, pod.Name, meta.AnnotationHibernate, valTrue, "suspended")
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
		if pod.Annotations[meta.AnnotationHibernate] == valTrue {
			c.patchPodAnnotation(ctx, ns, name, pod.Name, meta.AnnotationHibernate, "", "unsuspended")
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
func (c *controller) scaleSubAgents(ctx context.Context, cs *cocoonSet, ns, name string, agentPods map[int]*corev1.Pod, replicas int64) {
	logger := log.WithFunc("controller.scaleSubAgents")
	mainVMName := agentPods[0].Annotations[meta.AnnotationVMName]

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
func (c *controller) ensureToolboxes(ctx context.Context, cs *cocoonSet, ns, name string, toolboxPods map[string]*corev1.Pod, toolboxSpecs []cocoonToolboxSpec) {
	logger := log.WithFunc("controller.ensureToolboxes")
	for _, tb := range toolboxSpecs {
		tbName := tb.Name
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
		meta.AnnotationVMName: vmName,
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
		meta.LabelCocoonSet: csName,
		meta.LabelRole:      role,
		"app":               csName,
	}
}

func newManagedPod(cs *cocoonSet, podName, role, image, nodeName string, annotations map[string]string) *corev1.Pod {
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
func buildAgentPod(ctx context.Context, cs *cocoonSet, slot int, forkFrom string) *corev1.Pod {
	name := cs.Name
	ns := cs.Namespace
	agentSpec := cs.Spec.Agent
	nodeName := cs.Spec.targetNodeName()
	snapshotPolicy := cs.Spec.snapshotPolicy()

	image := agentSpec.Image
	storage := agentSpec.Storage

	podName := fmt.Sprintf("%s-%d", name, slot)
	vmName := meta.VMNameForDeployment(ns, name, slot)

	role := meta.RoleMain
	if slot > 0 {
		role = meta.RoleSubAgent
	}

	annotations := managedPodAnnotations(vmName, map[string]string{
		meta.AnnotationMode:           agentSpec.modeType(),
		meta.AnnotationImage:          image,
		meta.AnnotationManaged:        valTrue,
		meta.AnnotationOS:             agentSpec.osType(),
		meta.AnnotationStorage:        storage,
		meta.AnnotationSnapshotPolicy: snapshotPolicy,
		"cocoon.cis/network":          agentSpec.Network,
	})
	if forkFrom != "" && slot > 0 {
		annotations[meta.AnnotationForkFrom] = forkFrom
	}

	pod := newManagedPod(cs, podName, role, image, nodeName, annotations)
	pod.Labels[meta.LabelSlot] = strconv.Itoa(slot)

	if agentSpec.osType() == "windows" {
		pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{
			Name:          "rdp",
			ContainerPort: 3389,
			Protocol:      corev1.ProtocolTCP,
		}}
	}

	applyResources(ctx, &pod.Spec.Containers[0], agentSpec.Resources)
	applyEnvFrom(&pod.Spec.Containers[0], agentSpec.EnvFrom)

	if agentSpec.ServiceAccountName != "" {
		pod.Spec.ServiceAccountName = agentSpec.ServiceAccountName
	}

	return pod
}

// buildToolboxPod creates a Pod for a toolbox entry.
func buildToolboxPod(ctx context.Context, cs *cocoonSet, tb cocoonToolboxSpec) *corev1.Pod {
	csName := cs.Name
	ns := cs.Namespace
	nodeName := cs.Spec.targetNodeName()
	snapshotPolicy := cs.Spec.snapshotPolicy()

	tbName := tb.Name
	tbOS := tb.osType()
	tbImage := tb.Image
	tbMode := tb.mode()
	tbStorage := tb.Storage
	statusHints := cs.Status.toolboxRuntimeHints(tbName)

	podName := fmt.Sprintf("%s-%s", csName, tbName)
	vmName := meta.VMNameForPod(ns, tbName)

	annotations := managedPodAnnotations(vmName, map[string]string{
		meta.AnnotationMode:           tbMode,
		meta.AnnotationManaged:        valTrue,
		meta.AnnotationOS:             tbOS,
		meta.AnnotationImage:          tbImage,
		meta.AnnotationStorage:        tbStorage,
		meta.AnnotationSnapshotPolicy: snapshotPolicy,
	})

	if tbMode == "static" {
		applyStaticHints(annotations, tb, statusHints)
	}

	pod := newManagedPod(cs, podName, meta.RoleToolbox, tbImage, nodeName, annotations)

	applyResources(ctx, &pod.Spec.Containers[0], tb.Resources)

	return pod
}

// ---------- CocoonSet status ----------

// updateCocoonSetStatus patches the status subresource of a CocoonSet.
func (c *controller) updateCocoonSetStatus(ctx context.Context, ns, name string, status cocoonSetStatus) error {
	return c.patchStatus(ctx, csGVR, ns, name, status)
}

// podIP returns the pod's IP, preferring PodIP over the annotation fallback.
func podIP(pod *corev1.Pod) string {
	if ip := pod.Status.PodIP; ip != "" {
		return ip
	}
	return pod.Annotations[meta.AnnotationIP]
}

// buildCocoonSetStatus builds a status map from current pod state.
func buildCocoonSetStatus(phase string, pods []corev1.Pod, csName string, desiredAgents int) cocoonSetStatus {
	var agents []cocoonSetAgentStatus
	var toolboxes []cocoonSetToolboxStatus
	readyAgents := 0
	prefix := len(csName) + 1

	for i := range pods {
		pod := &pods[i]
		role := pod.Labels[meta.LabelRole]

		podPhase := string(pod.Status.Phase)
		if podPhase == "" {
			podPhase = "Pending"
		}

		switch role {
		case meta.RoleMain, meta.RoleSubAgent:
			slot := 0
			if s, ok := pod.Labels[meta.LabelSlot]; ok {
				slot, _ = strconv.Atoi(s)
			}
			if pod.Status.Phase == corev1.PodRunning {
				readyAgents++
			}
			agent := cocoonSetAgentStatus{
				Slot:    int64(slot),
				Role:    role,
				PodName: pod.Name,
				VMName:  pod.Annotations[meta.AnnotationVMName],
				Phase:   podPhase,
			}
			if vmID, ok := pod.Annotations[meta.AnnotationVMID]; ok {
				agent.VMID = vmID
			}
			if ip := podIP(pod); ip != "" {
				agent.IP = ip
			}
			if forkFrom, ok := pod.Annotations[meta.AnnotationForkFrom]; ok && forkFrom != "" {
				agent.ForkedFrom = forkFrom
			}
			agents = append(agents, agent)

		case meta.RoleToolbox:
			if prefix > len(pod.Name) {
				continue
			}
			tbName := pod.Name[prefix:]
			tb := cocoonSetToolboxStatus{
				Name:    tbName,
				PodName: pod.Name,
				VMName:  pod.Annotations[meta.AnnotationVMName],
				Phase:   podPhase,
			}
			if ip := podIP(pod); ip != "" {
				tb.IP = ip
			}
			if vmID, ok := pod.Annotations[meta.AnnotationVMID]; ok && vmID != "" {
				tb.VMID = vmID
			}
			_, hasVNCPort := pod.Annotations[meta.AnnotationVNCPort]
			tb.ConnType = toolboxConnType(pod.Annotations[meta.AnnotationOS], hasVNCPort)
			if hasVNCPort {
				if port, err := strconv.Atoi(pod.Annotations[meta.AnnotationVNCPort]); err == nil {
					tb.VNCPort = int64(port)
				}
			}
			toolboxes = append(toolboxes, tb)
		}
	}

	status := cocoonSetStatus{
		Phase:         phase,
		ReadyAgents:   int64(readyAgents),
		DesiredAgents: int64(desiredAgents),
	}
	if agents != nil {
		status.Agents = agents
	}
	if toolboxes != nil {
		status.Toolboxes = toolboxes
	}
	return status
}

// ---------- Helpers ----------

// patchPodAnnotation sets or removes an annotation on a pod.
// Pass a non-empty value to set, or an empty string to remove the annotation.
func (c *controller) patchPodAnnotation(ctx context.Context, ns, csName, podName, key, value, verb string) { //nolint:unparam // key is parameterized for reuse across annotation types
	logger := log.WithFunc("controller.patchPodAnnotation")
	patch, err := commonk8s.AnnotationsMergePatch(map[string]any{key: annotationPatchValue(value)})
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
		LabelSelector: fmt.Sprintf("%s=%s", meta.LabelCocoonSet, csName),
	})
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// ownerRef builds the standard OwnerReference slice for a CocoonSet-owned pod.
func ownerRef(cs *cocoonSet) []metav1.OwnerReference {
	blockOwnerDeletion := true
	isController := true
	return []metav1.OwnerReference{
		{
			APIVersion:         meta.APIVersion,
			Kind:               meta.KindCocoonSet,
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
			Key:      meta.TolerationKey,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}
}

// applyResources copies CPU/memory limits from an unstructured map to a container.
func applyResources(ctx context.Context, container *corev1.Container, resources resourceHints) {
	if resources.CPU != "" {
		if container.Resources.Limits == nil {
			container.Resources.Limits = corev1.ResourceList{}
		}
		container.Resources.Limits[corev1.ResourceCPU] = mustParseQuantity(ctx, resources.CPU)
	}
	if resources.Memory != "" {
		if container.Resources.Limits == nil {
			container.Resources.Limits = corev1.ResourceList{}
		}
		container.Resources.Limits[corev1.ResourceMemory] = mustParseQuantity(ctx, resources.Memory)
	}
}

// applyEnvFrom copies envFrom sources from the typed agent spec to a container.
func applyEnvFrom(container *corev1.Container, refs []envFromSourceSpec) {
	container.EnvFrom = append(container.EnvFrom, envFromSource(refs)...)
}

// applyStaticHints sets IP, VMID, and VNC port annotations for static toolboxes,
// preferring runtime status hints over spec hints.
func applyStaticHints(annotations map[string]string, tb cocoonToolboxSpec, statusHints *cocoonSetToolboxStatus) {
	if statusHints != nil && statusHints.IP != "" {
		annotations[meta.AnnotationIP] = statusHints.IP
	} else if tb.StaticIP != "" {
		annotations[meta.AnnotationIP] = tb.StaticIP
	}
	if statusHints != nil && statusHints.VMID != "" {
		annotations[meta.AnnotationVMID] = statusHints.VMID
	} else if tb.StaticVMID != "" {
		annotations[meta.AnnotationVMID] = tb.StaticVMID
	}
	if statusHints != nil && statusHints.VNCPort > 0 {
		annotations[meta.AnnotationVNCPort] = strconv.FormatInt(statusHints.VNCPort, 10)
	} else if tb.VNCPort > 0 {
		annotations[meta.AnnotationVNCPort] = strconv.FormatInt(tb.VNCPort, 10)
	}
}

func toolboxConnType(osType string, hasVNCPort bool) string {
	return meta.ConnectionType(osType, hasVNCPort)
}
