package cocoonset

import (
	"context"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

const (
	conditionTypeReady       = "Ready"
	conditionTypeProgressing = "Progressing"

	conditionReasonAllReady    = "AllAgentsReady"
	conditionReasonNotReady    = "AgentsNotReady"
	conditionReasonStable      = "Stable"
	conditionReasonReconciling = "Reconciling"
)

// patchStatus writes the supplied status onto the CocoonSet via the
// /status subresource. It diff-checks first so reconciles that did
// not actually change anything stay no-ops at the API server. The
// timestamps inside Conditions survive the diff because buildStatus
// builds them through apimeta.SetStatusCondition, which preserves
// the existing LastTransitionTime when nothing else changed.
func (r *Reconciler) patchStatus(ctx context.Context, cs *cocoonv1.CocoonSet, status cocoonv1.CocoonSetStatus) error {
	mergeConditions(&status, cs.Status.Conditions)
	if equality.Semantic.DeepEqual(cs.Status, status) {
		return nil
	}
	patch := client.MergeFrom(cs.DeepCopy())
	cs.Status = status
	if err := r.Status().Patch(ctx, cs, patch); err != nil {
		return fmt.Errorf("patch status %s/%s: %w", cs.Namespace, cs.Name, err)
	}
	return nil
}

// mergeConditions takes the freshly-built conditions on next and
// runs them through apimeta.SetStatusCondition against a copy of
// prev. The result is that timestamps survive when nothing else
// about a condition changed, so equality.Semantic.DeepEqual can
// catch the no-op case without a hand-rolled comparator.
func mergeConditions(next *cocoonv1.CocoonSetStatus, prev []metav1.Condition) {
	merged := make([]metav1.Condition, 0, len(prev))
	merged = append(merged, prev...)
	for _, c := range next.Conditions {
		apimeta.SetStatusCondition(&merged, c)
	}
	next.Conditions = merged
}

// buildStatus rebuilds the CocoonSetStatus from the supplied
// classified-pods snapshot. When phase is empty the running-state
// phase is auto-derived from the (ready, desired) counts; the suspend
// short-circuit and pending-main paths pass an explicit override.
//
// One pass over classified.sub computes the ready count and the
// AgentStatus list together so the reconcile path never walks the
// same map twice on the stable path.
func buildStatus(cs *cocoonv1.CocoonSet, classified classifiedPods, phase cocoonv1.CocoonSetPhase) cocoonv1.CocoonSetStatus {
	desired := int32(1) + cs.Spec.Agent.Replicas
	ready := int32(0)

	agents := make([]cocoonv1.AgentStatus, 0, desired)
	if classified.main != nil {
		if isPodReady(classified.main) {
			ready++
		}
		agents = append(agents, agentStatusFromPod(classified.main, 0, meta.RoleMain, ""))
	}

	mainVMName := ""
	if classified.main != nil {
		mainVMName = meta.ParseVMSpec(classified.main).VMName
	}
	for _, slot := range slices.Sorted(maps.Keys(classified.sub)) {
		sub := classified.sub[slot]
		if isPodReady(sub) {
			ready++
		}
		agents = append(agents, agentStatusFromPod(sub, slot, meta.RoleSubAgent, mainVMName))
	}

	tbStatuses := make([]cocoonv1.ToolboxStatus, 0, len(classified.toolbox))
	for _, name := range slices.Sorted(maps.Keys(classified.toolbox)) {
		tbStatuses = append(tbStatuses, toolboxStatusFromPod(classified.toolbox[name], name))
	}

	if phase == "" {
		phase = derivePhase(classified.main, ready, desired)
	}

	return cocoonv1.CocoonSetStatus{
		ObservedGeneration: cs.Generation,
		Phase:              phase,
		ReadyAgents:        ready,
		DesiredAgents:      desired,
		Agents:             agents,
		Toolboxes:          tbStatuses,
		Conditions:         buildConditions(cs, ready, desired, phase),
	}
}

// derivePhase reports the running-state phase implied by the
// (main, ready, desired) triple. Used by buildStatus on the no-override
// path.
func derivePhase(main *corev1.Pod, ready, desired int32) cocoonv1.CocoonSetPhase {
	switch {
	case main == nil:
		return cocoonv1.CocoonSetPhasePending
	case ready < desired:
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
		ConnType: meta.ConnectionType(spec.OS, vmRuntime.VNCPort > 0),
		VNCPort:  vmRuntime.VNCPort,
	}
}

// buildConditions returns the freshly-computed Ready and
// Progressing conditions for the supplied phase. Timestamps are
// left zero so apimeta.SetStatusCondition (called from
// mergeConditions on the patchStatus path) preserves the existing
// LastTransitionTime when nothing else changed.
func buildConditions(cs *cocoonv1.CocoonSet, ready, desired int32, phase cocoonv1.CocoonSetPhase) []metav1.Condition {
	readyCond := metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             conditionReasonNotReady,
		Message:            fmt.Sprintf("%d/%d agents ready", ready, desired),
		ObservedGeneration: cs.Generation,
	}
	if ready == desired && desired > 0 {
		readyCond.Status = metav1.ConditionTrue
		readyCond.Reason = conditionReasonAllReady
	}

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
