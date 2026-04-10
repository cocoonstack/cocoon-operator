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

// testScheme returns a scheme with corev1 + cocoonv1 types
// registered, matching what the operator's main wires up so the
// pod builders can resolve the controller reference.
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cocoonv1.AddToScheme(scheme))
	return scheme
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
	pod := buildAgentPod(cs, 0, "", "", testScheme(t))

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
	pod := buildAgentPod(cs, 1, mainVMName, "cocoonset-node-2", testScheme(t))

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
	pod := buildAgentPod(cs, 0, "", "", testScheme(t))
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
	pod := buildAgentPod(cs, 0, "", "", testScheme(t))
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
	pod := buildToolboxPod(cs, tb, testScheme(t))

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
	pod := buildToolboxPod(cs, tb, testScheme(t))
	if pod.Annotations[meta.AnnotationManaged] != "true" {
		t.Errorf("non-static toolbox should be managed")
	}
}

func TestClassifyPodsGroupsByRole(t *testing.T) {
	cs := newCocoonSet("demo")
	scheme := testScheme(t)
	main := buildAgentPod(cs, 0, "", "", scheme)
	sub1 := buildAgentPod(cs, 1, "vk-ns-demo-0", "", scheme)
	sub2 := buildAgentPod(cs, 2, "vk-ns-demo-0", "", scheme)
	tb := buildToolboxPod(cs, cocoonv1.ToolboxSpec{Name: "tb", Image: "x"}, scheme)

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
	pod := newManagedPod(cs, "demo-0", meta.RoleMain, "0", testScheme(t))
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
	pod := newManagedPod(cs, "demo-0", meta.RoleMain, "0", testScheme(t))
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
