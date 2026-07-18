// Package cocoonset hosts the CocoonSet reconciler and pod builder helpers.
package cocoonset

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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
	allByName map[string]*corev1.Pod
}

// forEachSorted invokes fn for every pod in allByName in name-sorted order,
// returning early on ctx cancellation or fn error.
func (c classifiedPods) forEachSorted(ctx context.Context, fn func(*corev1.Pod) error) error {
	for _, name := range slices.Sorted(maps.Keys(c.allByName)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(c.allByName[name]); err != nil {
			return err
		}
	}
	return nil
}

// classifyPods groups pods by role label; pods with an unknown role or an
// unparsable slot stay visible through allByName only.
func classifyPods(pods []corev1.Pod) classifiedPods {
	out := classifiedPods{
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{},
	}
	for i := range pods {
		p := &pods[i]
		// Skip pods already being deleted to prevent re-delete loops.
		if !p.DeletionTimestamp.IsZero() {
			continue
		}
		out.allByName[p.Name] = p
		switch p.Labels[meta.LabelRole] {
		case meta.RoleMain:
			out.main = p
		case meta.RoleSubAgent:
			if slot, err := strconv.ParseInt(p.Labels[meta.LabelSlot], 10, 32); err == nil {
				out.sub[int32(slot)] = p
			}
		case meta.RoleToolbox:
			out.toolbox[p.Labels[meta.LabelSlot]] = p
		}
	}
	return out
}

// buildAgentPod constructs the desired Pod for an agent slot.
// Slot 0 is main; slot >= 1 are sub-agents that fork from the main VM.
func buildAgentPod(cs *cocoonv1.CocoonSet, slot int32, mainVMName, bindNodeName string, scheme *runtime.Scheme) (*corev1.Pod, error) {
	role := meta.RoleMain
	forkFrom := ""
	if slot > 0 {
		role = meta.RoleSubAgent
		forkFrom = mainVMName
	}

	vmName := meta.VMNameForDeployment(cs.Namespace, cs.Name, int(slot))
	podName := fmt.Sprintf("%s-%d", cs.Name, slot)

	pod, err := newManagedPod(cs, podName, role, strconv.FormatInt(int64(slot), 10), scheme)
	if err != nil {
		return nil, err
	}

	meta.FromAgentSpec(cs.Spec.Agent, vmName, cs.Spec.SnapshotPolicy, forkFrom).Apply(pod)

	pod.Spec.Containers[0].Resources = cs.Spec.Agent.Resources
	applyStorageRequest(pod, cs.Spec.Agent.Storage)
	pod.Spec.Containers[0].EnvFrom = cs.Spec.Agent.EnvFrom
	if cs.Spec.Agent.ServiceAccountName != "" {
		pod.Spec.ServiceAccountName = cs.Spec.Agent.ServiceAccountName
	}
	if bindNodeName != "" {
		pod.Spec.NodeName = bindNodeName
	} else if slot == 0 && cs.Spec.NodeName != "" {
		// Scheduler affinity, not a hard NodeName bind: the main lands only if
		// it fits and the node is schedulable, else stays Pending.
		pod.Spec.Affinity = hostnameAffinity(cs.Spec.NodeName)
	}
	return pod, nil
}

func hostnameAffinity(nodeName string) *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      corev1.LabelHostname,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{nodeName},
					}},
				}},
			},
		},
	}
}

func buildToolboxPod(cs *cocoonv1.CocoonSet, tb cocoonv1.ToolboxSpec, scheme *runtime.Scheme) (*corev1.Pod, error) {
	podName := toolboxPodName(cs.Name, tb.Name)
	vmName := meta.VMNameForPod(cs.Namespace, podName)

	pod, err := newManagedPod(cs, podName, meta.RoleToolbox, tb.Name, scheme)
	if err != nil {
		return nil, err
	}

	meta.FromToolboxSpec(tb, vmName, cs.Spec.SnapshotPolicy).Apply(pod)

	if tb.Mode == cocoonv1.ToolboxModeStatic {
		vmRuntime := meta.VMRuntime{VMID: tb.StaticVMID, IP: tb.StaticIP, VNCPort: tb.VNCPort}
		vmRuntime.Apply(pod)
	}
	pod.Spec.Containers[0].Resources = tb.Resources
	applyStorageRequest(pod, tb.Storage)
	return pod, nil
}

// toolboxPodName is the deterministic pod name for a toolbox, shared by the
// builder and the collision check so the two cannot diverge.
func toolboxPodName(csName, tbName string) string {
	return fmt.Sprintf("%s-%s", csName, tbName)
}

func newManagedPod(cs *cocoonv1.CocoonSet, podName, role, slotLabel string, scheme *runtime.Scheme) (*corev1.Pod, error) {
	one := int64(1)
	pool := cmp.Or(cs.Spec.NodePool, meta.DefaultNodePool)
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
			Annotations: map[string]string{
				meta.AnnotationCocoonSetGeneration: strconv.FormatInt(cs.Generation, 10),
			},
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &one,
			Tolerations: []corev1.Toleration{
				{Key: meta.TolerationKey, Operator: corev1.TolerationOpExists},
				{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
				{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
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
		return nil, fmt.Errorf("set controller reference: %w", err)
	}
	return pod, nil
}

// podSpecMatchesAgent reports whether a running agent pod still matches what
// buildAgentPod would produce from the current CocoonSet spec.
func podSpecMatchesAgent(pod *corev1.Pod, cs *cocoonv1.CocoonSet, slot int32) bool {
	current := meta.ParseVMSpec(pod)
	// Sub-agents inherit ForkFrom; main agents leave it empty so a manual edit drifts.
	forkFrom := ""
	if slot > 0 {
		forkFrom = current.ForkFrom
	}
	want := meta.FromAgentSpec(cs.Spec.Agent, current.VMName, cs.Spec.SnapshotPolicy, forkFrom)
	if !vmSpecMatches(current, want) || !resourcesMatch(pod, cs.Spec.Agent.Resources) {
		return false
	}
	// K8s fills ServiceAccountName with "default" when unset; normalize
	// both sides so empty and "default" are treated as equivalent.
	podSA := cmp.Or(pod.Spec.ServiceAccountName, "default")
	wantSA := cmp.Or(cs.Spec.Agent.ServiceAccountName, "default")
	if podSA != wantSA {
		return false
	}
	if !equality.Semantic.DeepEqual(pod.Spec.Containers[0].EnvFrom, cs.Spec.Agent.EnvFrom) {
		return false
	}
	if !nodePoolMatches(pod, cs) {
		return false
	}
	return true
}

// podSpecMatchesToolbox reports whether a running toolbox pod still matches
// what buildToolboxPod would produce from the current CocoonSet spec.
func podSpecMatchesToolbox(pod *corev1.Pod, cs *cocoonv1.CocoonSet, tb cocoonv1.ToolboxSpec) bool {
	current := meta.ParseVMSpec(pod)
	want := meta.FromToolboxSpec(tb, current.VMName, cs.Spec.SnapshotPolicy)
	if !vmSpecMatches(current, want) || !resourcesMatch(pod, tb.Resources) {
		return false
	}
	if !nodePoolMatches(pod, cs) {
		return false
	}
	if tb.Mode == cocoonv1.ToolboxModeStatic {
		got := meta.ParseVMRuntime(pod)
		if got.VMID != tb.StaticVMID || got.IP != tb.StaticIP || got.VNCPort != tb.VNCPort {
			return false
		}
	}
	return true
}

// vmSpecMatches uses struct equality so any future VMSpec field is covered.
// Callers copy VMName/ForkFrom from current into want to avoid spurious mismatches.
func vmSpecMatches(got, want meta.VMSpec) bool {
	return got == want
}

func resourcesMatch(pod *corev1.Pod, want corev1.ResourceRequirements) bool {
	if len(pod.Spec.Containers) == 0 {
		return false
	}
	got := pod.Spec.Containers[0].Resources
	// Only CPU and memory: K8s defaulting copies the injected
	// ephemeral-storage into both Limits and Requests, which would always
	// mismatch a spec that never carries it.
	// Guaranteed-QoS defaulting fills Requests from Limits; mirror it.
	wantReq := want.Requests
	if len(wantReq) == 0 {
		wantReq = want.Limits
	}
	for _, res := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		if !quantityEqual(got.Limits, want.Limits, res) {
			return false
		}
		if !quantityEqual(got.Requests, wantReq, res) {
			return false
		}
	}
	return true
}

func quantityEqual(a, b corev1.ResourceList, name corev1.ResourceName) bool {
	qa, oka := a[name]
	qb, okb := b[name]
	if !oka && !okb {
		return true
	}
	if !oka || !okb {
		return false
	}
	return qa.Cmp(qb) == 0
}

func nodePoolMatches(pod *corev1.Pod, cs *cocoonv1.CocoonSet) bool {
	return meta.PodNodePool(pod) == cmp.Or(cs.Spec.NodePool, meta.DefaultNodePool)
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
	if c.Resources.Limits == nil {
		c.Resources.Limits = corev1.ResourceList{}
	}
	// Set both so Guaranteed QoS defaulting preserves the value.
	c.Resources.Requests[corev1.ResourceEphemeralStorage] = *storage
	c.Resources.Limits[corev1.ResourceEphemeralStorage] = *storage
}
