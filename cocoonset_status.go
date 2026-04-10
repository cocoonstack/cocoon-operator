package main

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1alpha1 "github.com/cocoonstack/cocoon-common/apis/v1alpha1"
	"github.com/cocoonstack/cocoon-common/meta"
)

// patchStatus writes the supplied status onto the CocoonSet via the
// /status subresource. It diff-checks first so reconciles that did
// not actually change anything do not bump .metadata.generation
// observers downstream.
func (r *CocoonSetReconciler) patchStatus(ctx context.Context, cs *cocoonv1alpha1.CocoonSet, status cocoonv1alpha1.CocoonSetStatus) error {
	if statusEqual(cs.Status, status) {
		return nil
	}
	patch := client.MergeFrom(cs.DeepCopy())
	cs.Status = status
	if err := r.Status().Patch(ctx, cs, patch); err != nil {
		return fmt.Errorf("patch status %s/%s: %w", cs.Namespace, cs.Name, err)
	}
	return nil
}

// buildStatus rebuilds the CocoonSetStatus from the supplied
// classified-pods snapshot. The phase argument lets the caller
// override the auto-derived phase (used by the suspend short-circuit
// and the pending-main path).
func buildStatus(cs *cocoonv1alpha1.CocoonSet, classified classifiedPods, phase cocoonv1alpha1.CocoonSetPhase) cocoonv1alpha1.CocoonSetStatus {
	desired := int32(1) + cs.Spec.Agent.Replicas
	ready := int32(0)
	if classified.main != nil && isPodReady(classified.main) {
		ready++
	}
	for _, p := range classified.sub {
		if isPodReady(p) {
			ready++
		}
	}

	agents := make([]cocoonv1alpha1.AgentStatus, 0, desired)
	if classified.main != nil {
		agents = append(agents, agentStatusFromPod(classified.main, 0, meta.RoleMain, ""))
	}
	subSlots := make([]int32, 0, len(classified.sub))
	for slot := range classified.sub {
		subSlots = append(subSlots, slot)
	}
	slices.Sort(subSlots)
	mainVMName := ""
	if classified.main != nil {
		mainVMName = meta.ParseVMSpec(classified.main).VMName
	}
	for _, slot := range subSlots {
		agents = append(agents, agentStatusFromPod(classified.sub[slot], slot, meta.RoleSubAgent, mainVMName))
	}

	tbStatuses := make([]cocoonv1alpha1.ToolboxStatus, 0, len(classified.toolbox))
	tbNames := make([]string, 0, len(classified.toolbox))
	for name := range classified.toolbox {
		tbNames = append(tbNames, name)
	}
	sort.Strings(tbNames)
	for _, name := range tbNames {
		tbStatuses = append(tbStatuses, toolboxStatusFromPod(classified.toolbox[name], name))
	}

	return cocoonv1alpha1.CocoonSetStatus{
		ObservedGeneration: cs.Generation,
		Phase:              phase,
		ReadyAgents:        ready,
		DesiredAgents:      desired,
		Agents:             agents,
		Toolboxes:          tbStatuses,
		Conditions:         buildConditions(cs, ready, desired, phase),
	}
}

// currentPhase derives the running-state phase from a classified
// snapshot when no override is in effect.
func currentPhase(cs *cocoonv1alpha1.CocoonSet, classified classifiedPods) cocoonv1alpha1.CocoonSetPhase {
	desired := int32(1) + cs.Spec.Agent.Replicas
	ready := int32(0)
	if classified.main != nil && isPodReady(classified.main) {
		ready++
	}
	for _, p := range classified.sub {
		if isPodReady(p) {
			ready++
		}
	}
	switch {
	case classified.main == nil:
		return cocoonv1alpha1.CocoonSetPhasePending
	case ready < desired:
		return cocoonv1alpha1.CocoonSetPhaseScaling
	default:
		return cocoonv1alpha1.CocoonSetPhaseRunning
	}
}

func agentStatusFromPod(pod *corev1.Pod, slot int32, role, forkedFrom string) cocoonv1alpha1.AgentStatus {
	spec := meta.ParseVMSpec(pod)
	runtime := meta.ParseVMRuntime(pod)
	return cocoonv1alpha1.AgentStatus{
		Slot:       slot,
		Role:       role,
		PodName:    pod.Name,
		VMName:     spec.VMName,
		VMID:       runtime.VMID,
		IP:         runtime.IP,
		Phase:      string(pod.Status.Phase),
		ForkedFrom: forkedFrom,
	}
}

func toolboxStatusFromPod(pod *corev1.Pod, name string) cocoonv1alpha1.ToolboxStatus {
	spec := meta.ParseVMSpec(pod)
	runtime := meta.ParseVMRuntime(pod)
	return cocoonv1alpha1.ToolboxStatus{
		Name:     name,
		PodName:  pod.Name,
		VMName:   spec.VMName,
		VMID:     runtime.VMID,
		IP:       runtime.IP,
		Phase:    string(pod.Status.Phase),
		ConnType: meta.ConnectionType(spec.OS, runtime.VNCPort > 0),
		VNCPort:  runtime.VNCPort,
	}
}

func buildConditions(cs *cocoonv1alpha1.CocoonSet, ready, desired int32, phase cocoonv1alpha1.CocoonSetPhase) []metav1.Condition {
	now := metav1.Now()
	readyCond := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "AgentsNotReady",
		Message:            fmt.Sprintf("%d/%d agents ready", ready, desired),
		LastTransitionTime: now,
		ObservedGeneration: cs.Generation,
	}
	if ready == desired && desired > 0 {
		readyCond.Status = metav1.ConditionTrue
		readyCond.Reason = "AllAgentsReady"
	}

	progressing := metav1.Condition{
		Type:               "Progressing",
		Status:             metav1.ConditionFalse,
		Reason:             "Stable",
		Message:            string(phase),
		LastTransitionTime: now,
		ObservedGeneration: cs.Generation,
	}
	if phase == cocoonv1alpha1.CocoonSetPhasePending || phase == cocoonv1alpha1.CocoonSetPhaseScaling {
		progressing.Status = metav1.ConditionTrue
		progressing.Reason = "Reconciling"
	}

	return []metav1.Condition{readyCond, progressing}
}

// statusEqual is a structural compare that ignores condition
// timestamps so the patchStatus diff doesn't fire on every reconcile
// just because metav1.Now() advanced.
func statusEqual(a, b cocoonv1alpha1.CocoonSetStatus) bool {
	if a.ObservedGeneration != b.ObservedGeneration ||
		a.Phase != b.Phase ||
		a.ReadyAgents != b.ReadyAgents ||
		a.DesiredAgents != b.DesiredAgents {
		return false
	}
	if !reflect.DeepEqual(a.Agents, b.Agents) {
		return false
	}
	if !reflect.DeepEqual(a.Toolboxes, b.Toolboxes) {
		return false
	}
	return conditionsEqualIgnoringTime(a.Conditions, b.Conditions)
}

func conditionsEqualIgnoringTime(a, b []metav1.Condition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type ||
			a[i].Status != b[i].Status ||
			a[i].Reason != b[i].Reason ||
			a[i].Message != b[i].Message ||
			a[i].ObservedGeneration != b[i].ObservedGeneration {
			return false
		}
	}
	return true
}
