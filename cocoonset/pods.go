// Package cocoonset hosts the CocoonSet reconciler and the pod
// builder helpers it relies on. The package is named after the CRD
// it manages so the import path reads naturally
// (`cocoonset.Reconciler`, `cocoonset.BuildAgentPod`).
package cocoonset

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
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
			// Toolbox pods reuse the slot label as the toolbox name
			// (operator side; classification mirror only). Storing
			// by that key keeps lookups O(1).
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
func buildAgentPod(cs *cocoonv1.CocoonSet, slot int32, mainVMName, bindNodeName string, scheme *runtime.Scheme) *corev1.Pod {
	role := meta.RoleMain
	forkFrom := ""
	if slot > 0 {
		role = meta.RoleSubAgent
		forkFrom = mainVMName
	}

	vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, int(slot))
	podName := fmt.Sprintf("%s-%d", cs.Name, slot)

	pod := newManagedPod(cs, podName, role, strconv.FormatInt(int64(slot), 10), scheme)

	spec := meta.VMSpec{
		VMName:         vmName,
		Image:          cs.Spec.Agent.Image,
		Mode:           string(cs.Spec.Agent.Mode.Default()),
		OS:             string(cs.Spec.Agent.OS.Default()),
		Network:        cs.Spec.Agent.Network,
		Storage:        quantityString(cs.Spec.Agent.Storage),
		SnapshotPolicy: string(cs.Spec.SnapshotPolicy.Default()),
		ForkFrom:       forkFrom,
		Managed:        true,
	}
	spec.Apply(pod)

	pod.Spec.Containers[0].Resources = cs.Spec.Agent.Resources
	pod.Spec.Containers[0].EnvFrom = cs.Spec.Agent.EnvFrom
	if cs.Spec.Agent.ServiceAccountName != "" {
		pod.Spec.ServiceAccountName = cs.Spec.Agent.ServiceAccountName
	}
	if bindNodeName != "" {
		pod.Spec.NodeName = bindNodeName
	}
	return pod
}

// buildToolboxPod constructs the desired Pod for a toolbox entry.
func buildToolboxPod(cs *cocoonv1.CocoonSet, tb cocoonv1.ToolboxSpec, scheme *runtime.Scheme) *corev1.Pod {
	vmName := meta.VMNameForPod(cs.Namespace, tb.Name)
	podName := fmt.Sprintf("%s-%s", cs.Name, tb.Name)

	pod := newManagedPod(cs, podName, meta.RoleToolbox, tb.Name, scheme)

	managed := tb.Mode != cocoonv1.ToolboxModeStatic
	spec := meta.VMSpec{
		VMName:         vmName,
		Image:          tb.Image,
		Mode:           string(tb.Mode.Default()),
		OS:             string(tb.OS.Default()),
		Storage:        quantityString(tb.Storage),
		SnapshotPolicy: string(cs.Spec.SnapshotPolicy.Default()),
		Managed:        managed,
	}
	spec.Apply(pod)

	if tb.Mode == cocoonv1.ToolboxModeStatic {
		// Static toolboxes carry pre-assigned runtime hints so
		// vk-cocoon does not invent its own.
		vmRuntime := meta.VMRuntime{VMID: tb.StaticVMID, IP: tb.StaticIP, VNCPort: tb.VNCPort}
		vmRuntime.Apply(pod)
	}
	pod.Spec.Containers[0].Resources = tb.Resources
	return pod
}

// newManagedPod returns a fresh Pod skeleton with the labels,
// owner-reference, toleration, and placeholder container that every
// CocoonSet-managed pod shares. controllerutil.SetControllerReference
// fills in APIVersion + Kind + Controller=true + BlockOwnerDeletion=true
// from the CocoonSet. A failure here is a programmer bug (missing
// type registration in the scheme), not a runtime condition, so
// panic instead of bubbling an error through every caller.
func newManagedPod(cs *cocoonv1.CocoonSet, podName, role, slotLabel string, scheme *runtime.Scheme) *corev1.Pod {
	one := int64(1)
	pool := cs.Spec.NodePool
	if pool == "" {
		pool = meta.DefaultNodePool
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: cs.Namespace,
			Labels: map[string]string{
				meta.LabelCocoonSet:      cs.Name,
				meta.LabelRole:           role,
				meta.LabelSlot:           slotLabel,
				"app.kubernetes.io/name": cs.Name,
			},
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &one,
			Tolerations: []corev1.Toleration{
				{Key: meta.TolerationKey, Operator: corev1.TolerationOpExists},
			},
			NodeSelector: map[string]string{
				meta.LabelNodePool: pool,
			},
			Containers: []corev1.Container{
				{
					Name:  agentContainerName,
					Image: placeholderImage,
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(cs, pod, scheme); err != nil {
		// CocoonSet is registered with the scheme in main.go's
		// buildScheme; failing here means a refactor mis-wired it.
		panic(fmt.Errorf("set controller reference: %w", err))
	}
	return pod
}

// quantityString returns the canonical string form of a resource
// quantity, or "" when the pointer is nil.
func quantityString(q *resource.Quantity) string {
	if q == nil {
		return ""
	}
	return q.String()
}
