package main

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cocoonv1alpha1 "github.com/cocoonstack/cocoon-common/apis/v1alpha1"
	"github.com/cocoonstack/cocoon-common/meta"
)

const (
	// agentContainerName is the name used for the placeholder
	// container in every managed pod. The actual VM is run by
	// vk-cocoon outside the container; the container exists so the
	// pod has a valid PodSpec.
	agentContainerName = "agent"

	// placeholderImage is the image cocoon pods carry. vk-cocoon
	// never pulls or runs it; it just needs a non-empty value to
	// satisfy the K8s admission controllers that validate PodSpec.
	placeholderImage = "ghcr.io/cocoonstack/placeholder:latest"
)

// classifiedPods is the result of grouping a CocoonSet's owned pods
// by their role label. main is the slot-0 pod, sub is every higher
// slot indexed by slot, and toolbox is keyed by toolbox name.
type classifiedPods struct {
	main      *corev1.Pod
	sub       map[int32]*corev1.Pod
	toolbox   map[string]*corev1.Pod
	unknowns  []*corev1.Pod
	allByName map[string]*corev1.Pod
}

// classifyPods groups the supplied pods by the cocoon role label.
// Pods that do not carry the cocoonset.cocoonstack.io/role label go
// into unknowns; the reconciler usually deletes them.
func classifyPods(pods []corev1.Pod) classifiedPods {
	out := classifiedPods{
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{},
	}
	for i := range pods {
		p := &pods[i]
		out.allByName[p.Name] = p
		role := p.Labels[meta.LabelRole]
		switch role {
		case meta.RoleMain:
			out.main = p
		case meta.RoleSubAgent:
			slot, err := strconv.ParseInt(p.Labels[meta.LabelSlot], 10, 32)
			if err != nil {
				out.unknowns = append(out.unknowns, p)
				continue
			}
			out.sub[int32(slot)] = p
		case meta.RoleToolbox:
			// Toolbox pods are named "<csname>-<toolboxname>"; we
			// rely on the LabelCocoonSet + the trailing pod-name
			// segment to recover the spec entry. Storing by name
			// keeps lookups O(1).
			out.toolbox[p.Labels[meta.LabelSlot]] = p
		default:
			out.unknowns = append(out.unknowns, p)
		}
	}
	return out
}

// buildAgentPod constructs the desired Pod for an agent slot.
// slot 0 is the main agent; slot >= 1 are sub-agents that fork from
// the main agent's VM.
func buildAgentPod(cs *cocoonv1alpha1.CocoonSet, slot int32, mainVMName string) *corev1.Pod {
	role := meta.RoleMain
	forkFrom := ""
	if slot > 0 {
		role = meta.RoleSubAgent
		forkFrom = mainVMName
	}

	vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, int(slot))
	podName := fmt.Sprintf("%s-%d", cs.Name, slot)

	pod := newManagedPod(cs, podName, role, strconv.FormatInt(int64(slot), 10))

	spec := meta.VMSpec{
		VMName:         vmName,
		Image:          cs.Spec.Agent.Image,
		Mode:           string(defaultedAgentMode(cs.Spec.Agent.Mode)),
		OS:             string(defaultedOSType(cs.Spec.Agent.OS, cocoonv1alpha1.OSLinux)),
		Network:        cs.Spec.Agent.Network,
		Storage:        quantityString(cs.Spec.Agent.Storage),
		SnapshotPolicy: string(defaultedSnapshotPolicy(cs.Spec.SnapshotPolicy)),
		ForkFrom:       forkFrom,
		Managed:        true,
	}
	spec.Apply(pod)

	pod.Spec.Containers[0].Resources = cs.Spec.Agent.Resources
	pod.Spec.Containers[0].EnvFrom = cs.Spec.Agent.EnvFrom
	if cs.Spec.Agent.ServiceAccountName != "" {
		pod.Spec.ServiceAccountName = cs.Spec.Agent.ServiceAccountName
	}
	return pod
}

// buildToolboxPod constructs the desired Pod for a toolbox entry.
func buildToolboxPod(cs *cocoonv1alpha1.CocoonSet, tb cocoonv1alpha1.ToolboxSpec) *corev1.Pod {
	vmName := meta.VMNameForPod(cs.Namespace, tb.Name)
	podName := fmt.Sprintf("%s-%s", cs.Name, tb.Name)

	pod := newManagedPod(cs, podName, meta.RoleToolbox, tb.Name)

	managed := tb.Mode != cocoonv1alpha1.ToolboxModeStatic
	spec := meta.VMSpec{
		VMName:         vmName,
		Image:          tb.Image,
		Mode:           string(defaultedToolboxMode(tb.Mode)),
		OS:             string(defaultedOSType(tb.OS, cocoonv1alpha1.OSLinux)),
		Storage:        quantityString(tb.Storage),
		SnapshotPolicy: string(defaultedSnapshotPolicy(cs.Spec.SnapshotPolicy)),
		Managed:        managed,
	}
	spec.Apply(pod)

	if tb.Mode == cocoonv1alpha1.ToolboxModeStatic {
		// Static toolboxes carry pre-assigned runtime hints so
		// vk-cocoon does not invent its own.
		runtime := meta.VMRuntime{VMID: tb.StaticVMID, IP: tb.StaticIP, VNCPort: tb.VNCPort}
		runtime.Apply(pod)
	}
	pod.Spec.Containers[0].Resources = tb.Resources
	return pod
}

// newManagedPod returns a fresh Pod skeleton with the labels,
// owner-reference, toleration, and placeholder container that every
// CocoonSet-managed pod shares. The slotLabel argument is the value
// for meta.LabelSlot — for sub-agents this is the slot number, for
// toolboxes it is the toolbox name.
func newManagedPod(cs *cocoonv1alpha1.CocoonSet, podName, role, slotLabel string) *corev1.Pod {
	one := int64(1)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: cs.Namespace,
			Labels: map[string]string{
				meta.LabelCocoonSet:      cs.Name,
				meta.LabelRole:           role,
				meta.LabelSlot:           slotLabel,
				"app.kubernetes.io/name": cs.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         meta.APIVersion,
					Kind:               meta.KindCocoonSet,
					Name:               cs.Name,
					UID:                cs.UID,
					Controller:         pointerBool(true),
					BlockOwnerDeletion: pointerBool(true),
				},
			},
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &one,
			Tolerations: []corev1.Toleration{
				{Key: meta.TolerationKey, Operator: corev1.TolerationOpExists},
			},
			NodeSelector: map[string]string{
				"cocoonstack.io/pool": defaultedNodePool(cs.Spec.NodePool),
			},
			Containers: []corev1.Container{
				{
					Name:  agentContainerName,
					Image: placeholderImage,
				},
			},
		},
	}
}

func pointerBool(v bool) *bool {
	return &v
}

func defaultedAgentMode(m cocoonv1alpha1.AgentMode) cocoonv1alpha1.AgentMode {
	if m == "" {
		return cocoonv1alpha1.AgentModeClone
	}
	return m
}

func defaultedToolboxMode(m cocoonv1alpha1.ToolboxMode) cocoonv1alpha1.ToolboxMode {
	if m == "" {
		return cocoonv1alpha1.ToolboxModeRun
	}
	return m
}

func defaultedOSType(o, fallback cocoonv1alpha1.OSType) cocoonv1alpha1.OSType {
	if o == "" {
		return fallback
	}
	return o
}

func defaultedSnapshotPolicy(p cocoonv1alpha1.SnapshotPolicy) cocoonv1alpha1.SnapshotPolicy {
	if p == "" {
		return cocoonv1alpha1.SnapshotPolicyAlways
	}
	return p
}

func defaultedNodePool(p string) string {
	if p == "" {
		return "default"
	}
	return p
}

// quantityString returns the canonical string form of a resource
// quantity, or "" when the pointer is nil.
func quantityString(q interface{ String() string }) string {
	if q == nil {
		return ""
	}
	return q.String()
}
