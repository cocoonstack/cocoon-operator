// Package cocoonset hosts the CocoonSet reconciler and pod builder helpers.
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
	agentContainerName = "agent"
	// placeholderImage is never pulled; it satisfies PodSpec validation.
	placeholderImage = "ghcr.io/cocoonstack/placeholder:latest"
)

type classifiedPods struct {
	main      *corev1.Pod
	sub       map[int32]*corev1.Pod
	toolbox   map[string]*corev1.Pod
	unknowns  []*corev1.Pod
	allByName map[string]*corev1.Pod
}

// classifyPods groups pods by role label; unlabelled pods go into unknowns.
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
			out.toolbox[p.Labels[meta.LabelSlot]] = p
		default:
			out.unknowns = append(out.unknowns, p)
		}
	}
	return out
}

// buildAgentPod constructs the desired Pod for an agent slot.
// Slot 0 is main; slot >= 1 are sub-agents that fork from the main VM.
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

	meta.FromAgentSpec(cs.Spec.Agent, vmName, cs.Spec.SnapshotPolicy, forkFrom).Apply(pod)

	pod.Spec.Containers[0].Resources = cs.Spec.Agent.Resources
	applyStorageRequest(pod, cs.Spec.Agent.Storage)
	pod.Spec.Containers[0].EnvFrom = cs.Spec.Agent.EnvFrom
	if cs.Spec.Agent.ServiceAccountName != "" {
		pod.Spec.ServiceAccountName = cs.Spec.Agent.ServiceAccountName
	}
	if bindNodeName != "" {
		pod.Spec.NodeName = bindNodeName
	}
	return pod
}

func buildToolboxPod(cs *cocoonv1.CocoonSet, tb cocoonv1.ToolboxSpec, scheme *runtime.Scheme) *corev1.Pod {
	vmName := meta.VMNameForPod(cs.Namespace, tb.Name)
	podName := fmt.Sprintf("%s-%s", cs.Name, tb.Name)

	pod := newManagedPod(cs, podName, meta.RoleToolbox, tb.Name, scheme)

	meta.FromToolboxSpec(tb, vmName, cs.Spec.SnapshotPolicy).Apply(pod)

	if tb.Mode == cocoonv1.ToolboxModeStatic {
		vmRuntime := meta.VMRuntime{VMID: tb.StaticVMID, IP: tb.StaticIP, VNCPort: tb.VNCPort}
		vmRuntime.Apply(pod)
	}
	pod.Spec.Containers[0].Resources = tb.Resources
	applyStorageRequest(pod, tb.Storage)
	return pod
}

// newManagedPod returns a Pod skeleton with shared labels, owner-reference,
// toleration, and placeholder container. Panics on scheme mis-wiring.
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
		panic(fmt.Errorf("set controller reference: %w", err))
	}
	return pod
}

// applyStorageRequest propagates the VMOptions.Storage quantity into the
// pod's ephemeral-storage resource request so the K8s scheduler can
// account for VM disk usage. No-op when storage is nil.
func applyStorageRequest(pod *corev1.Pod, storage *resource.Quantity) {
	if storage == nil || storage.IsZero() {
		return
	}
	c := &pod.Spec.Containers[0]
	if c.Resources.Requests == nil {
		c.Resources.Requests = corev1.ResourceList{}
	}
	c.Resources.Requests[corev1.ResourceEphemeralStorage] = *storage
}
