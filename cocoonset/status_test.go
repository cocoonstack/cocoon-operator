package cocoonset

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

func readyPod(p *corev1.Pod) *corev1.Pod {
	p.Status.Phase = corev1.PodRunning
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	})
	return p
}

func TestBuildStatusPendingWhenNoMain(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})
	classified := classifiedPods{sub: map[int32]*corev1.Pod{}, toolbox: map[string]*corev1.Pod{}, allByName: map[string]*corev1.Pod{}}
	if got := buildStatus(cs, classified, "").Phase; got != cocoonv1.CocoonSetPhasePending {
		t.Errorf("phase: %q, want Pending", got)
	}
}

func TestBuildStatusScalingWhenSubsMissing(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})
	main := readyPod(buildAgentPod(cs, 0, "", testScheme(t)))
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main},
	}
	if got := buildStatus(cs, classified, "").Phase; got != cocoonv1.CocoonSetPhaseScaling {
		t.Errorf("phase: %q, want Scaling", got)
	}
}

func TestBuildStatusRunningWhenAllReady(t *testing.T) {
	cs := newCocoonSet("demo")
	main := readyPod(buildAgentPod(cs, 0, "", testScheme(t)))
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main},
	}
	if got := buildStatus(cs, classified, "").Phase; got != cocoonv1.CocoonSetPhaseRunning {
		t.Errorf("phase: %q, want Running", got)
	}
}

func TestBuildStatusReportsAgents(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 2
	})
	main := readyPod(buildAgentPod(cs, 0, "", testScheme(t)))
	sub1 := readyPod(buildAgentPod(cs, 1, "vk-ns-demo-0", testScheme(t)))
	classified := classifiedPods{
		main:      main,
		sub:       map[int32]*corev1.Pod{1: sub1},
		toolbox:   map[string]*corev1.Pod{},
		allByName: map[string]*corev1.Pod{main.Name: main, sub1.Name: sub1},
	}
	status := buildStatus(cs, classified, "")
	if status.DesiredAgents != 3 {
		t.Errorf("desired: %d, want 3", status.DesiredAgents)
	}
	if status.ReadyAgents != 2 {
		t.Errorf("ready: %d, want 2", status.ReadyAgents)
	}
	if len(status.Agents) != 2 {
		t.Errorf("agents: %d, want 2", len(status.Agents))
	}
	if status.Agents[1].ForkedFrom != "vk-ns-demo-0" {
		t.Errorf("forkedFrom: %q", status.Agents[1].ForkedFrom)
	}
}

func TestStatusEqualIgnoresConditionTimestamps(t *testing.T) {
	cs := newCocoonSet("demo")
	a := buildStatus(cs, classifiedPods{
		sub: map[int32]*corev1.Pod{}, toolbox: map[string]*corev1.Pod{}, allByName: map[string]*corev1.Pod{},
	}, cocoonv1.CocoonSetPhasePending)
	b := buildStatus(cs, classifiedPods{
		sub: map[int32]*corev1.Pod{}, toolbox: map[string]*corev1.Pod{}, allByName: map[string]*corev1.Pod{},
	}, cocoonv1.CocoonSetPhasePending)
	// Conditions carry zero timestamps from buildStatus; semantic
	// deep-equal must agree two identical builds are equivalent.
	if !equality.Semantic.DeepEqual(a, b) {
		t.Errorf("equality.Semantic.DeepEqual must accept identical builds")
	}
}

func TestStatusEqualDetectsChange(t *testing.T) {
	cs := newCocoonSet("demo")
	a := buildStatus(cs, classifiedPods{
		sub: map[int32]*corev1.Pod{}, toolbox: map[string]*corev1.Pod{}, allByName: map[string]*corev1.Pod{},
	}, cocoonv1.CocoonSetPhasePending)
	b := buildStatus(cs, classifiedPods{
		sub: map[int32]*corev1.Pod{}, toolbox: map[string]*corev1.Pod{}, allByName: map[string]*corev1.Pod{},
	}, cocoonv1.CocoonSetPhaseRunning)
	if equality.Semantic.DeepEqual(a, b) {
		t.Errorf("equality.Semantic.DeepEqual should detect phase change")
	}
}

func TestAgentStatusFromPod(t *testing.T) {
	cs := newCocoonSet("demo")
	pod := buildAgentPod(cs, 0, "", testScheme(t))
	pod.Status.Phase = corev1.PodRunning
	vmRuntime := meta.VMRuntime{VMID: "qemu-1", IP: "10.0.0.1"}
	vmRuntime.Apply(pod)
	st := agentStatusFromPod(pod, 0, meta.RoleMain, "")
	if st.VMName != "vk-ns-demo-0" {
		t.Errorf("vmName: %q", st.VMName)
	}
	if st.VMID != "qemu-1" {
		t.Errorf("vmID: %q", st.VMID)
	}
	if st.IP != "10.0.0.1" {
		t.Errorf("ip: %q", st.IP)
	}
	if st.Phase != string(corev1.PodRunning) {
		t.Errorf("phase: %q", st.Phase)
	}
}

func TestBuildConditionsAllReady(t *testing.T) {
	cs := newCocoonSet("demo")
	conds := buildConditions(cs, 1, 1, cocoonv1.CocoonSetPhaseRunning)
	var ready *metav1.Condition
	for i := range conds {
		if conds[i].Type == conditionTypeReady {
			ready = &conds[i]
		}
	}
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready condition should be True when all agents ready, got %+v", ready)
	}
}
