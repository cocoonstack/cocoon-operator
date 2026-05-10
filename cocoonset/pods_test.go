package cocoonset

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	"github.com/cocoonstack/cocoon-common/meta"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cocoonv1.AddToScheme(scheme))
	return scheme
}

func mustBuildAgentPod(t *testing.T, cs *cocoonv1.CocoonSet, slot int32, mainVMName, bindNodeName string, scheme *runtime.Scheme) *corev1.Pod {
	t.Helper()
	pod, err := buildAgentPod(cs, slot, mainVMName, bindNodeName, scheme)
	if err != nil {
		t.Fatalf("build agent pod: %v", err)
	}
	return pod
}

func mustBuildToolboxPod(t *testing.T, cs *cocoonv1.CocoonSet, tb cocoonv1.ToolboxSpec, scheme *runtime.Scheme) *corev1.Pod {
	t.Helper()
	pod, err := buildToolboxPod(cs, tb, scheme)
	if err != nil {
		t.Fatalf("build toolbox pod: %v", err)
	}
	return pod
}

func mustNewManagedPod(t *testing.T, cs *cocoonv1.CocoonSet, podName, role, slotLabel string, scheme *runtime.Scheme) *corev1.Pod {
	t.Helper()
	pod, err := newManagedPod(cs, podName, role, slotLabel, scheme)
	if err != nil {
		t.Fatalf("new managed pod: %v", err)
	}
	return pod
}

func newCocoonSet(name string, modifiers ...func(*cocoonv1.CocoonSet)) *cocoonv1.CocoonSet {
	cs := &cocoonv1.CocoonSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: "test-uid"},
		Spec: cocoonv1.CocoonSetSpec{
			Agent: cocoonv1.AgentSpec{
				Image: "ghcr.io/cocoonstack/cocoon/ubuntu:24.04",
			},
		},
	}
	for _, m := range modifiers {
		m(cs)
	}
	return cs
}

func TestBuildAgentPodSlot0IsMain(t *testing.T) {
	cs := newCocoonSet("demo")
	pod := mustBuildAgentPod(t, cs, 0, "", "", testScheme(t))

	if pod.Name != "demo-0" {
		t.Errorf("pod name: %q, want demo-0", pod.Name)
	}
	if pod.Labels[meta.LabelRole] != meta.RoleMain {
		t.Errorf("role label: %q, want main", pod.Labels[meta.LabelRole])
	}
	if pod.Labels[meta.LabelSlot] != "0" {
		t.Errorf("slot label: %q, want 0", pod.Labels[meta.LabelSlot])
	}
	if pod.Annotations[meta.AnnotationVMName] != "vk-ns-demo-0" {
		t.Errorf("vmname: %q", pod.Annotations[meta.AnnotationVMName])
	}
	if pod.Annotations[meta.AnnotationForkFrom] != "" {
		t.Errorf("main agent must not carry a fork-from annotation, got %q", pod.Annotations[meta.AnnotationForkFrom])
	}
}

func TestBuildAgentPodSubAgentForksFromMain(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 2
	})
	mainVMName := "vk-ns-demo-0"
	pod := mustBuildAgentPod(t, cs, 1, mainVMName, "cocoonset-node-2", testScheme(t))

	if pod.Labels[meta.LabelRole] != meta.RoleSubAgent {
		t.Errorf("role: %q, want sub-agent", pod.Labels[meta.LabelRole])
	}
	if pod.Annotations[meta.AnnotationForkFrom] != mainVMName {
		t.Errorf("fork-from: %q, want %q", pod.Annotations[meta.AnnotationForkFrom], mainVMName)
	}
	if pod.Spec.NodeName != "cocoonset-node-2" {
		t.Errorf("nodeName: %q, want cocoonset-node-2", pod.Spec.NodeName)
	}
}

func TestBuildAgentPodAppliesAgentDefaults(t *testing.T) {
	cs := newCocoonSet("demo")
	pod := mustBuildAgentPod(t, cs, 0, "", "", testScheme(t))
	if pod.Annotations[meta.AnnotationMode] != string(cocoonv1.AgentModeClone) {
		t.Errorf("mode default: %q", pod.Annotations[meta.AnnotationMode])
	}
	if pod.Annotations[meta.AnnotationOS] != string(cocoonv1.OSLinux) {
		t.Errorf("os default: %q", pod.Annotations[meta.AnnotationOS])
	}
	if pod.Annotations[meta.AnnotationManaged] != "true" {
		t.Errorf("managed should be true, got %q", pod.Annotations[meta.AnnotationManaged])
	}
}

func TestBuildAgentPodPropagatesStorage(t *testing.T) {
	q := resource.MustParse("100Gi")
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Storage = &q
	})
	pod := mustBuildAgentPod(t, cs, 0, "", "", testScheme(t))
	if pod.Annotations[meta.AnnotationStorage] != "100Gi" {
		t.Errorf("storage: %q", pod.Annotations[meta.AnnotationStorage])
	}
}

func TestBuildToolboxPodStaticCarriesRuntimeHints(t *testing.T) {
	cs := newCocoonSet("demo")
	tb := cocoonv1.ToolboxSpec{
		Name:       "tb",
		Mode:       cocoonv1.ToolboxModeStatic,
		StaticIP:   "10.0.0.1",
		StaticVMID: "qemu-1",
		VNCPort:    5901,
	}
	pod := mustBuildToolboxPod(t, cs, tb, testScheme(t))

	if pod.Annotations[meta.AnnotationManaged] == "true" {
		t.Errorf("static toolbox should not be marked managed")
	}
	if pod.Annotations[meta.AnnotationVMID] != "qemu-1" {
		t.Errorf("vmid: %q", pod.Annotations[meta.AnnotationVMID])
	}
	if pod.Annotations[meta.AnnotationIP] != "10.0.0.1" {
		t.Errorf("ip: %q", pod.Annotations[meta.AnnotationIP])
	}
	if pod.Annotations[meta.AnnotationVNCPort] != "5901" {
		t.Errorf("vncport: %q", pod.Annotations[meta.AnnotationVNCPort])
	}
}

func TestBuildToolboxPodNonStaticIsManaged(t *testing.T) {
	cs := newCocoonSet("demo")
	tb := cocoonv1.ToolboxSpec{
		Name:  "tb",
		Mode:  cocoonv1.ToolboxModeRun,
		Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest",
	}
	pod := mustBuildToolboxPod(t, cs, tb, testScheme(t))
	if pod.Annotations[meta.AnnotationManaged] != "true" {
		t.Errorf("non-static toolbox should be managed")
	}
}

func TestClassifyPodsGroupsByRole(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	main := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	sub1 := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0", "", scheme)
	sub2 := mustBuildAgentPod(t, cs, 2, "vk-ns-demo-0", "", scheme)
	tb := mustBuildToolboxPod(t, cs, cocoonv1.ToolboxSpec{Name: "tb", Image: "x"}, scheme)

	pods := []corev1.Pod{*main, *sub1, *sub2, *tb}
	got := classifyPods(pods)

	if got.main == nil || got.main.Name != "demo-0" {
		t.Errorf("main not classified correctly")
	}
	if len(got.sub) != 2 {
		t.Errorf("sub count: %d, want 2", len(got.sub))
	}
	if _, ok := got.sub[1]; !ok {
		t.Errorf("missing slot 1")
	}
	if len(got.toolbox) != 1 {
		t.Errorf("toolbox count: %d, want 1", len(got.toolbox))
	}
	if _, ok := got.toolbox["tb"]; !ok {
		t.Errorf("toolbox tb missing")
	}
}

func TestClassifyPodsUnknownsBucket(t *testing.T) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "stranger",
			Labels: map[string]string{}, // no role label
		},
	}
	got := classifyPods([]corev1.Pod{pod})
	if len(got.unknowns) != 1 {
		t.Errorf("unknowns: %d, want 1", len(got.unknowns))
	}
}

func TestNewManagedPodHasOwnerReference(t *testing.T) {
	cs := newCocoonSet("demo")
	pod := mustNewManagedPod(t, cs, "demo-0", meta.RoleMain, "0", testScheme(t))
	if len(pod.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner ref, got %d", len(pod.OwnerReferences))
	}
	ref := pod.OwnerReferences[0]
	if ref.Kind != meta.KindCocoonSet || ref.Name != "demo" {
		t.Errorf("owner ref: %+v", ref)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("owner ref must be controller")
	}
}

func TestNewManagedPodCarriesCocoonToleration(t *testing.T) {
	cs := newCocoonSet("demo")
	pod := mustNewManagedPod(t, cs, "demo-0", meta.RoleMain, "0", testScheme(t))
	found := false
	for _, tol := range pod.Spec.Tolerations {
		if tol.Key == meta.TolerationKey {
			found = true
		}
	}
	if !found {
		t.Errorf("toleration %s missing", meta.TolerationKey)
	}
}

func TestNewManagedPodStampsCocoonSetGeneration(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 42
	})
	pod := mustNewManagedPod(t, cs, "demo-0", meta.RoleMain, "0", testScheme(t))
	if got := pod.Annotations[meta.AnnotationCocoonSetGeneration]; got != "42" {
		t.Errorf("cocoonset generation annotation = %q, want 42", got)
	}
}

// Guards against a future cocoon-common change where VMSpec.Apply replaces
// pod.Annotations instead of merging — that would silently strip the
// generation stamp set by newManagedPod.
func TestBuildAgentPodPreservesCocoonSetGeneration(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 11
	})
	pod := mustBuildAgentPod(t, cs, 0, "", "", testScheme(t))
	if got := pod.Annotations[meta.AnnotationCocoonSetGeneration]; got != "11" {
		t.Errorf("cocoonset generation annotation = %q, want 11", got)
	}
}

func TestBuildToolboxPodPreservesCocoonSetGeneration(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Generation = 13
	})
	tb := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest"}
	pod := mustBuildToolboxPod(t, cs, tb, testScheme(t))
	if got := pod.Annotations[meta.AnnotationCocoonSetGeneration]; got != "13" {
		t.Errorf("cocoonset generation annotation = %q, want 13", got)
	}
}

func TestPodSpecMatchesAgentIdenticalSpec(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	if !podSpecMatchesAgent(pod, cs, 0) {
		t.Error("freshly built pod should match its own spec")
	}
}

func TestPodSpecMatchesAgentDetectsImageDrift(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)

	updated := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Image = "ghcr.io/cocoonstack/cocoon/ubuntu:26.04"
	})
	if podSpecMatchesAgent(pod, updated, 0) {
		t.Error("pod with old image should not match updated spec")
	}
}

func TestPodSpecMatchesAgentDetectsBackendDrift(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)

	updated := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Backend = "firecracker"
	})
	if podSpecMatchesAgent(pod, updated, 0) {
		t.Error("pod with old backend should not match updated spec")
	}
}

func TestPodSpecMatchesAgentDetectsResourceDrift(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		}
	})
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)

	updated := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
		}
	})
	if podSpecMatchesAgent(pod, updated, 0) {
		t.Error("pod with old resources should not match updated spec")
	}
}

func TestPodSpecMatchesAgentDetectsProbePortDrift(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)

	updated := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.ProbePort = 8080
	})
	if podSpecMatchesAgent(pod, updated, 0) {
		t.Error("pod without probePort should not match spec that sets probePort")
	}
}

func TestPodSpecMatchesToolboxDetectsProbePortDrift(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	tb := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest", Mode: cocoonv1.ToolboxModeRun}
	pod := mustBuildToolboxPod(t, cs, tb, scheme)

	updatedTb := tb
	updatedTb.ProbePort = 9090
	if podSpecMatchesToolbox(pod, cs, updatedTb) {
		t.Error("toolbox pod without probePort should not match spec that sets probePort")
	}
}

func TestPodSpecMatchesToolboxIdenticalSpec(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	tb := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest", Mode: cocoonv1.ToolboxModeRun}
	pod := mustBuildToolboxPod(t, cs, tb, scheme)
	if !podSpecMatchesToolbox(pod, cs, tb) {
		t.Error("freshly built toolbox pod should match its own spec")
	}
}

func TestPodSpecMatchesToolboxDetectsImageDrift(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	tb := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:v1"}
	pod := mustBuildToolboxPod(t, cs, tb, scheme)

	updatedTb := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:v2"}
	if podSpecMatchesToolbox(pod, cs, updatedTb) {
		t.Error("toolbox pod with old image should not match updated spec")
	}
}

func TestPodSpecMatchesSubAgentPreservesForkFrom(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.Replicas = 1
	})
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 1, "vk-ns-demo-0", "", scheme)
	if !podSpecMatchesAgent(pod, cs, 1) {
		t.Error("sub-agent should match when spec is unchanged")
	}
}

func TestPodSpecMatchesAgentDetectsServiceAccountDrift(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.ServiceAccountName = "sa-old"
	})
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)

	updated := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.ServiceAccountName = "sa-new"
	})
	if podSpecMatchesAgent(pod, updated, 0) {
		t.Error("pod with old service account should not match updated spec")
	}
}

func TestPodSpecMatchesAgentDetectsEnvFromDrift(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.EnvFrom = []corev1.EnvFromSource{
			{ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cm-a"},
			}},
		}
	})
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)

	updated := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.Agent.EnvFrom = []corev1.EnvFromSource{
			{ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cm-b"},
			}},
		}
	})
	if podSpecMatchesAgent(pod, updated, 0) {
		t.Error("pod with old envFrom should not match updated spec")
	}
}

func TestPodSpecMatchesAgentDetectsNodePoolDrift(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.NodePool = "gpu"
	})
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)

	updated := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.NodePool = "cpu"
	})
	if podSpecMatchesAgent(pod, updated, 0) {
		t.Error("pod with old node pool should not match updated spec")
	}
}

func TestPodSpecMatchesAgentNodePoolDefaultFallback(t *testing.T) {
	cs := newCocoonSet("demo") // NodePool empty -> DefaultNodePool
	scheme := testScheme(t)
	pod := mustBuildAgentPod(t, cs, 0, "", "", scheme)
	if !podSpecMatchesAgent(pod, cs, 0) {
		t.Error("pod with default node pool should match spec without explicit pool")
	}
}

func TestPodSpecMatchesToolboxDetectsStaticVMIDDrift(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	tb := cocoonv1.ToolboxSpec{
		Name:       "tb",
		Mode:       cocoonv1.ToolboxModeStatic,
		StaticVMID: "qemu-1",
		StaticIP:   "10.0.0.1",
		VNCPort:    5901,
	}
	pod := mustBuildToolboxPod(t, cs, tb, scheme)

	updatedTb := cocoonv1.ToolboxSpec{
		Name:       "tb",
		Mode:       cocoonv1.ToolboxModeStatic,
		StaticVMID: "qemu-2",
		StaticIP:   "10.0.0.1",
		VNCPort:    5901,
	}
	if podSpecMatchesToolbox(pod, cs, updatedTb) {
		t.Error("toolbox pod with old static VMID should not match updated spec")
	}
}

func TestPodSpecMatchesToolboxDetectsStaticIPDrift(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	tb := cocoonv1.ToolboxSpec{
		Name:       "tb",
		Mode:       cocoonv1.ToolboxModeStatic,
		StaticVMID: "qemu-1",
		StaticIP:   "10.0.0.1",
		VNCPort:    5901,
	}
	pod := mustBuildToolboxPod(t, cs, tb, scheme)

	updatedTb := cocoonv1.ToolboxSpec{
		Name:       "tb",
		Mode:       cocoonv1.ToolboxModeStatic,
		StaticVMID: "qemu-1",
		StaticIP:   "10.0.0.2",
		VNCPort:    5901,
	}
	if podSpecMatchesToolbox(pod, cs, updatedTb) {
		t.Error("toolbox pod with old static IP should not match updated spec")
	}
}

func TestPodSpecMatchesToolboxDetectsVNCPortDrift(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	tb := cocoonv1.ToolboxSpec{
		Name:       "tb",
		Mode:       cocoonv1.ToolboxModeStatic,
		StaticVMID: "qemu-1",
		StaticIP:   "10.0.0.1",
		VNCPort:    5901,
	}
	pod := mustBuildToolboxPod(t, cs, tb, scheme)

	updatedTb := cocoonv1.ToolboxSpec{
		Name:       "tb",
		Mode:       cocoonv1.ToolboxModeStatic,
		StaticVMID: "qemu-1",
		StaticIP:   "10.0.0.1",
		VNCPort:    5902,
	}
	if podSpecMatchesToolbox(pod, cs, updatedTb) {
		t.Error("toolbox pod with old VNC port should not match updated spec")
	}
}

func TestPodSpecMatchesToolboxStaticIdenticalSpec(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	tb := cocoonv1.ToolboxSpec{
		Name:       "tb",
		Mode:       cocoonv1.ToolboxModeStatic,
		StaticVMID: "qemu-1",
		StaticIP:   "10.0.0.1",
		VNCPort:    5901,
	}
	pod := mustBuildToolboxPod(t, cs, tb, scheme)
	if !podSpecMatchesToolbox(pod, cs, tb) {
		t.Error("freshly built static toolbox pod should match its own spec")
	}
}

func TestPodSpecMatchesToolboxDetectsNodePoolDrift(t *testing.T) {
	cs := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.NodePool = "gpu"
	})
	scheme := testScheme(t)
	tb := cocoonv1.ToolboxSpec{Name: "tb", Image: "ghcr.io/cocoonstack/cocoon/toolbox:latest", Mode: cocoonv1.ToolboxModeRun}
	pod := mustBuildToolboxPod(t, cs, tb, scheme)

	updated := newCocoonSet("demo", func(cs *cocoonv1.CocoonSet) {
		cs.Spec.NodePool = "cpu"
	})
	if podSpecMatchesToolbox(pod, updated, tb) {
		t.Error("toolbox pod with old node pool should not match updated spec")
	}
}
