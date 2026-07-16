package cocoonset

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	"github.com/cocoonstack/cocoon-common/meta"
)

const (
	conditionTypeProgressing = "Progressing"

	conditionReasonAllReady    = "AllAgentsReady"
	conditionReasonNotReady    = "AgentsNotReady"
	conditionReasonStable      = "Stable"
	conditionReasonReconciling = "Reconciling"
)

// patchStatus writes status via the /status subresource, skipping no-op updates.
func (r *Reconciler) patchStatus(ctx context.Context, cs *cocoonv1.CocoonSet, status cocoonv1.CocoonSetStatus) error {
	mergeConditions(&status, cs.Status.Conditions)
	if equality.Semantic.DeepEqual(cs.Status, status) {
		return nil
	}
	if err := commonk8s.PatchStatus(ctx, r.Client, cs, func(cs *cocoonv1.CocoonSet) {
		cs.Status = status
	}); err != nil {
		return fmt.Errorf("patch status %s/%s: %w", cs.Namespace, cs.Name, err)
	}
	return nil
}

// mergeConditions preserves condition timestamps across no-op updates.
func mergeConditions(next *cocoonv1.CocoonSetStatus, prev []metav1.Condition) {
	merged := slices.Clone(prev)
	for _, c := range next.Conditions {
		apimeta.SetStatusCondition(&merged, c)
	}
	next.Conditions = merged
}

// buildStatus rebuilds CocoonSetStatus from classified pods. Empty phase is auto-derived.
func buildStatus(cs *cocoonv1.CocoonSet, classified classifiedPods, phase cocoonv1.CocoonSetPhase) cocoonv1.CocoonSetStatus {
	desired := int32(1) + cs.Spec.Agent.Replicas
	ready := int32(0)
	mainVMName := ""

	agents := make([]cocoonv1.AgentStatus, 0, desired)
	if classified.main != nil {
		if meta.IsPodReady(classified.main) {
			ready++
		}
		mainVMName = meta.ParseVMSpec(classified.main).VMName
		agents = append(agents, agentStatusFromPod(classified.main, 0, meta.RoleMain, ""))
	}

	for _, slot := range slices.Sorted(maps.Keys(classified.sub)) {
		sub := classified.sub[slot]
		if meta.IsPodReady(sub) {
			ready++
		}
		agents = append(agents, agentStatusFromPod(sub, slot, meta.RoleSubAgent, mainVMName))
	}

	tbDesired := int32(len(cs.Spec.Toolboxes)) //nolint:gosec // bounded by CocoonSet spec size
	tbReady := int32(0)
	tbStatuses := make([]cocoonv1.ToolboxStatus, 0, len(classified.toolbox))
	for _, name := range slices.Sorted(maps.Keys(classified.toolbox)) {
		pod := classified.toolbox[name]
		if meta.IsPodReady(pod) {
			tbReady++
		}
		tbStatuses = append(tbStatuses, toolboxStatusFromPod(pod, name))
	}

	phase = cmp.Or(phase, derivePhase(classified.main, ready, desired, tbReady, tbDesired))

	return cocoonv1.CocoonSetStatus{
		ObservedGeneration: cs.Generation,
		Phase:              phase,
		ReadyAgents:        ready,
		DesiredAgents:      desired,
		ReadyToolboxes:     tbReady,
		DesiredToolboxes:   tbDesired,
		Agents:             agents,
		Toolboxes:          tbStatuses,
		Conditions:         buildConditions(cs, ready, desired, tbReady, tbDesired, phase),
	}
}

func derivePhase(main *corev1.Pod, ready, desired, tbReady, tbDesired int32) cocoonv1.CocoonSetPhase {
	switch {
	case main == nil:
		return cocoonv1.CocoonSetPhasePending
	case ready < desired, tbReady < tbDesired:
		return cocoonv1.CocoonSetPhaseScaling
	default:
		return cocoonv1.CocoonSetPhaseRunning
	}
}

func agentStatusFromPod(pod *corev1.Pod, slot int32, role, forkedFrom string) cocoonv1.AgentStatus {
	spec := meta.ParseVMSpec(pod)
	vmRuntime := meta.ParseVMRuntime(pod)
	return cocoonv1.AgentStatus{
		Slot:       slot,
		Role:       role,
		PodName:    pod.Name,
		VMName:     spec.VMName,
		VMID:       vmRuntime.VMID,
		IP:         vmRuntime.IP,
		Phase:      string(pod.Status.Phase),
		ForkedFrom: forkedFrom,
	}
}

func toolboxStatusFromPod(pod *corev1.Pod, name string) cocoonv1.ToolboxStatus {
	spec := meta.ParseVMSpec(pod)
	vmRuntime := meta.ParseVMRuntime(pod)
	return cocoonv1.ToolboxStatus{
		Name:     name,
		PodName:  pod.Name,
		VMName:   spec.VMName,
		VMID:     vmRuntime.VMID,
		IP:       vmRuntime.IP,
		Phase:    string(pod.Status.Phase),
		ConnType: cocoonv1.ConnType(meta.ConnectionType(spec.OS, vmRuntime.VNCPort > 0, spec.ConnType)),
		VNCPort:  vmRuntime.VNCPort,
	}
}

// buildConditions returns Ready and Progressing conditions.
// Timestamps are left zero so mergeConditions preserves existing LastTransitionTime.
func buildConditions(cs *cocoonv1.CocoonSet, ready, desired, tbReady, tbDesired int32, phase cocoonv1.CocoonSetPhase) []metav1.Condition {
	readyStatus := metav1.ConditionFalse
	readyReason := conditionReasonNotReady
	if ready == desired && desired > 0 && tbReady == tbDesired {
		readyStatus = metav1.ConditionTrue
		readyReason = conditionReasonAllReady
	}
	readyCond := commonk8s.NewReadyCondition(
		cs.Generation,
		readyStatus,
		readyReason,
		fmt.Sprintf("%d/%d agents ready, %d/%d toolboxes ready", ready, desired, tbReady, tbDesired),
	)

	progressing := metav1.Condition{
		Type:               conditionTypeProgressing,
		Status:             metav1.ConditionFalse,
		Reason:             conditionReasonStable,
		Message:            string(phase),
		ObservedGeneration: cs.Generation,
	}
	if phase == cocoonv1.CocoonSetPhasePending || phase == cocoonv1.CocoonSetPhaseScaling {
		progressing.Status = metav1.ConditionTrue
		progressing.Reason = conditionReasonReconciling
	}

	return []metav1.Condition{readyCond, progressing}
}
