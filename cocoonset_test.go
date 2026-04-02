package main

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildToolboxPodIgnoresStaticHintsForManagedWindows(t *testing.T) {
	cs := &unstructured.Unstructured{}
	cs.SetName("demo")
	cs.SetNamespace("dev")

	tb := map[string]any{
		"name":       "windows",
		"os":         "windows",
		"mode":       "run",
		"image":      "win1125h2",
		"staticIP":   "10.88.100.68",
		"staticVMID": "qemu-windows",
		"vncPort":    int64(5901),
	}

	ctx := context.Background()
	pod := buildToolboxPod(ctx, cs, tb)
	if got := pod.Annotations[annMode]; got != "run" {
		t.Fatalf("mode mismatch: got %q", got)
	}
	if _, ok := pod.Annotations[annIP]; ok {
		t.Fatalf("unexpected static IP annotation for managed toolbox")
	}
	if _, ok := pod.Annotations[annVMID]; ok {
		t.Fatalf("unexpected static VMID annotation for managed toolbox")
	}
	if _, ok := pod.Annotations[annVNCPort]; ok {
		t.Fatalf("unexpected VNC annotation for managed toolbox")
	}
	if got := pod.Spec.NodeName; got != "cocoon-pool" {
		t.Fatalf("default node name mismatch: got %q", got)
	}
}

func TestBuildToolboxPodKeepsStaticHintsForStaticMode(t *testing.T) {
	cs := &unstructured.Unstructured{}
	cs.SetName("demo")
	cs.SetNamespace("dev")

	tb := map[string]any{
		"name":       "windows",
		"os":         "windows",
		"mode":       "static",
		"image":      "windows-server-2022",
		"staticIP":   "10.88.100.68",
		"staticVMID": "qemu-windows",
		"vncPort":    int64(5901),
	}

	ctx := context.Background()
	pod := buildToolboxPod(ctx, cs, tb)
	if got := pod.Annotations[annIP]; got != "10.88.100.68" {
		t.Fatalf("static IP mismatch: got %q", got)
	}
	if got := pod.Annotations[annVMID]; got != "qemu-windows" {
		t.Fatalf("static VMID mismatch: got %q", got)
	}
	if got := pod.Annotations[annVNCPort]; got != "5901" {
		t.Fatalf("VNC port mismatch: got %q", got)
	}
}

func TestBuildToolboxPodPrefersRuntimeStatusHintsForStaticMode(t *testing.T) {
	cs := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"toolboxes": []any{
				map[string]any{
					"name":    "windows",
					"ip":      "10.88.100.85",
					"vmID":    "qemu-windows",
					"vncPort": int64(5902),
				},
			},
		},
	}}
	cs.SetName("demo")
	cs.SetNamespace("dev")

	tb := map[string]any{
		"name":       "windows",
		"os":         "windows",
		"mode":       "static",
		"image":      "windows-server-2022",
		"staticIP":   "10.88.100.68",
		"staticVMID": "wrong-vmid",
		"vncPort":    int64(5901),
	}

	ctx := context.Background()
	pod := buildToolboxPod(ctx, cs, tb)
	if got := pod.Annotations[annIP]; got != "10.88.100.85" {
		t.Fatalf("runtime status IP mismatch: got %q", got)
	}
	if got := pod.Annotations[annVMID]; got != "qemu-windows" {
		t.Fatalf("runtime status VMID mismatch: got %q", got)
	}
	if got := pod.Annotations[annVNCPort]; got != "5902" {
		t.Fatalf("runtime status VNC port mismatch: got %q", got)
	}
}

func TestToolboxConnType(t *testing.T) {
	tests := []struct {
		name       string
		osType     string
		hasVNCPort bool
		want       string
	}{
		{name: "windows managed", osType: "windows", want: "rdp"},
		{name: "windows static", osType: "windows", hasVNCPort: true, want: "vnc"},
		{name: "android managed", osType: "android", want: "adb"},
		{name: "android vnc", osType: "android", hasVNCPort: true, want: "vnc"},
		{name: "linux", osType: "linux", want: "ssh"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toolboxConnType(tt.osType, tt.hasVNCPort); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildCocoonSetStatusIncludesToolboxVMID(t *testing.T) {
	pods := []map[string]string{
		{
			"name":   "demo-windows",
			"role":   "toolbox",
			"vmName": "demo-windows",
			"vmID":   "qemu-windows",
			"ip":     "10.88.100.85",
			"os":     "windows",
			"vnc":    "5902",
		},
	}

	var kubePods []corev1.Pod
	for _, p := range pods {
		kubePods = append(kubePods, corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: p["name"],
				Labels: map[string]string{
					labelRole: p["role"],
				},
				Annotations: map[string]string{
					annVMName:  p["vmName"],
					annVMID:    p["vmID"],
					annIP:      p["ip"],
					annOS:      p["os"],
					annVNCPort: p["vnc"],
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: p["ip"],
			},
		})
	}

	status := buildCocoonSetStatus("Running", kubePods, "demo", 1)
	toolboxes, ok := status["toolboxes"].([]any)
	if !ok || len(toolboxes) != 1 {
		t.Fatalf("unexpected toolboxes status: %#v", status["toolboxes"])
	}
	tb, ok := toolboxes[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected toolbox entry: %#v", toolboxes[0])
	}
	if got := tb["vmID"]; got != "qemu-windows" {
		t.Fatalf("toolbox vmID mismatch: got %#v", got)
	}
}

func TestBuildAgentPodUsesConfiguredNodeName(t *testing.T) {
	cs := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{
				"nodeName": "cocoon-pool-233",
				"agent": map[string]any{
					"image": "https://registry.example.com/demo-linux-base",
				},
			},
		},
	}
	cs.SetName("demo")
	cs.SetNamespace("dev")

	ctx := context.Background()
	pod := buildAgentPod(ctx, cs, 0, "")
	if got := pod.Spec.NodeName; got != "cocoon-pool-233" {
		t.Fatalf("agent node name mismatch: got %q", got)
	}
}

func TestBuildToolboxPodUsesConfiguredNodeName(t *testing.T) {
	cs := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{
				"nodeName": "cocoon-pool-233",
			},
		},
	}
	cs.SetName("demo")
	cs.SetNamespace("dev")

	tb := map[string]any{
		"name":  "windows",
		"os":    "windows",
		"mode":  "run",
		"image": "https://registry.example.com/win11-base",
	}

	ctx := context.Background()
	pod := buildToolboxPod(ctx, cs, tb)
	if got := pod.Spec.NodeName; got != "cocoon-pool-233" {
		t.Fatalf("toolbox node name mismatch: got %q", got)
	}
}

func TestManagedPodAnnotationsSkipsEmptyValues(t *testing.T) {
	got := managedPodAnnotations("vk-dev-demo-0", map[string]string{
		annMode:    "clone",
		annImage:   "",
		annStorage: "10G",
	})

	want := map[string]string{
		annVMName:  "vk-dev-demo-0",
		annMode:    "clone",
		annStorage: "10G",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("managedPodAnnotations mismatch:\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestNewManagedPodSharesCommonSkeleton(t *testing.T) {
	cs := &unstructured.Unstructured{}
	cs.SetName("demo")
	cs.SetNamespace("dev")
	cs.SetUID("12345")

	annotations := managedPodAnnotations("vk-dev-demo-0", map[string]string{
		annMode: "clone",
	})
	pod := newManagedPod(cs, "demo-0", roleMain, "demo-image", "cocoon-pool-233", annotations)

	if got := pod.Namespace; got != "dev" {
		t.Fatalf("namespace mismatch: got %q", got)
	}
	if got := pod.Spec.NodeName; got != "cocoon-pool-233" {
		t.Fatalf("node name mismatch: got %q", got)
	}
	if got := pod.Labels[labelCocoonSet]; got != "demo" {
		t.Fatalf("cocoonset label mismatch: got %q", got)
	}
	if got := pod.Labels[labelRole]; got != roleMain {
		t.Fatalf("role label mismatch: got %q", got)
	}
	if got := pod.Labels["app"]; got != "demo" {
		t.Fatalf("app label mismatch: got %q", got)
	}
	if got := pod.Annotations[annVMName]; got != "vk-dev-demo-0" {
		t.Fatalf("vm name annotation mismatch: got %q", got)
	}
	if got := pod.Spec.Containers; len(got) != 1 || got[0].Name != "vm" || got[0].Image != "demo-image" {
		t.Fatalf("container skeleton mismatch: %#v", got)
	}
	if got := pod.Spec.Tolerations; len(got) != 1 || got[0].Key != "virtual-kubelet.io/provider" {
		t.Fatalf("tolerations mismatch: %#v", got)
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Kind != kindCocoonSet || pod.OwnerReferences[0].Name != "demo" {
		t.Fatalf("owner references mismatch: %#v", pod.OwnerReferences)
	}
}

func TestAnnotationPatchValue(t *testing.T) {
	if got := annotationPatchValue("value"); got != "value" {
		t.Fatalf("expected value, got %#v", got)
	}
	if got := annotationPatchValue(""); got != nil {
		t.Fatalf("expected nil for empty value, got %#v", got)
	}
}
