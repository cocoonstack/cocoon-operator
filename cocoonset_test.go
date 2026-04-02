package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/cocoonstack/cocoon-common/meta"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildToolboxPodIgnoresStaticHintsForManagedWindows(t *testing.T) {
	cs := &cocoonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "dev"},
	}

	tb := cocoonToolboxSpec{
		Name:       "windows",
		OS:         "windows",
		Mode:       "run",
		Image:      "win1125h2",
		StaticIP:   "10.88.100.68",
		StaticVMID: "qemu-windows",
		VNCPort:    5901,
	}

	ctx := context.Background()
	pod := buildToolboxPod(ctx, cs, tb)
	if got := pod.Annotations[meta.AnnotationMode]; got != "run" {
		t.Fatalf("mode mismatch: got %q", got)
	}
	if _, ok := pod.Annotations[meta.AnnotationIP]; ok {
		t.Fatalf("unexpected static IP annotation for managed toolbox")
	}
	if _, ok := pod.Annotations[meta.AnnotationVMID]; ok {
		t.Fatalf("unexpected static VMID annotation for managed toolbox")
	}
	if _, ok := pod.Annotations[meta.AnnotationVNCPort]; ok {
		t.Fatalf("unexpected VNC annotation for managed toolbox")
	}
	if got := pod.Spec.NodeName; got != "cocoon-pool" {
		t.Fatalf("default node name mismatch: got %q", got)
	}
}

func TestBuildToolboxPodKeepsStaticHintsForStaticMode(t *testing.T) {
	cs := &cocoonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "dev"},
	}

	tb := cocoonToolboxSpec{
		Name:       "windows",
		OS:         "windows",
		Mode:       "static",
		Image:      "windows-server-2022",
		StaticIP:   "10.88.100.68",
		StaticVMID: "qemu-windows",
		VNCPort:    5901,
	}

	ctx := context.Background()
	pod := buildToolboxPod(ctx, cs, tb)
	if got := pod.Annotations[meta.AnnotationIP]; got != "10.88.100.68" {
		t.Fatalf("static IP mismatch: got %q", got)
	}
	if got := pod.Annotations[meta.AnnotationVMID]; got != "qemu-windows" {
		t.Fatalf("static VMID mismatch: got %q", got)
	}
	if got := pod.Annotations[meta.AnnotationVNCPort]; got != "5901" {
		t.Fatalf("VNC port mismatch: got %q", got)
	}
}

func TestBuildToolboxPodPrefersRuntimeStatusHintsForStaticMode(t *testing.T) {
	cs := &cocoonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "dev"},
		Status: cocoonSetStatus{
			Toolboxes: []cocoonSetToolboxStatus{{
				Name:    "windows",
				IP:      "10.88.100.85",
				VMID:    "qemu-windows",
				VNCPort: 5902,
			}},
		},
	}

	tb := cocoonToolboxSpec{
		Name:       "windows",
		OS:         "windows",
		Mode:       "static",
		Image:      "windows-server-2022",
		StaticIP:   "10.88.100.68",
		StaticVMID: "wrong-vmid",
		VNCPort:    5901,
	}

	ctx := context.Background()
	pod := buildToolboxPod(ctx, cs, tb)
	if got := pod.Annotations[meta.AnnotationIP]; got != "10.88.100.85" {
		t.Fatalf("runtime status IP mismatch: got %q", got)
	}
	if got := pod.Annotations[meta.AnnotationVMID]; got != "qemu-windows" {
		t.Fatalf("runtime status VMID mismatch: got %q", got)
	}
	if got := pod.Annotations[meta.AnnotationVNCPort]; got != "5902" {
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
					meta.LabelRole: p["role"],
				},
				Annotations: map[string]string{
					meta.AnnotationVMName:  p["vmName"],
					meta.AnnotationVMID:    p["vmID"],
					meta.AnnotationIP:      p["ip"],
					meta.AnnotationOS:      p["os"],
					meta.AnnotationVNCPort: p["vnc"],
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: p["ip"],
			},
		})
	}

	status := buildCocoonSetStatus("Running", kubePods, "demo", 1)
	if len(status.Toolboxes) != 1 {
		t.Fatalf("unexpected toolboxes status: %#v", status.Toolboxes)
	}
	if got := status.Toolboxes[0].VMID; got != "qemu-windows" {
		t.Fatalf("toolbox vmID mismatch: got %#v", got)
	}
}

func TestBuildAgentPodUsesConfiguredNodeName(t *testing.T) {
	cs := &cocoonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "dev"},
		Spec: cocoonSetSpec{
			NodeName: "cocoon-pool-233",
			Agent: cocoonSetAgentSpec{
				Image: "https://registry.example.com/demo-linux-base",
			},
		},
	}

	ctx := context.Background()
	pod := buildAgentPod(ctx, cs, 0, "")
	if got := pod.Spec.NodeName; got != "cocoon-pool-233" {
		t.Fatalf("agent node name mismatch: got %q", got)
	}
}

func TestBuildToolboxPodUsesConfiguredNodeName(t *testing.T) {
	cs := &cocoonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "dev"},
		Spec: cocoonSetSpec{
			NodeName: "cocoon-pool-233",
		},
	}

	tb := cocoonToolboxSpec{
		Name:  "windows",
		OS:    "windows",
		Mode:  "run",
		Image: "https://registry.example.com/win11-base",
	}

	ctx := context.Background()
	pod := buildToolboxPod(ctx, cs, tb)
	if got := pod.Spec.NodeName; got != "cocoon-pool-233" {
		t.Fatalf("toolbox node name mismatch: got %q", got)
	}
}

func TestManagedPodAnnotationsSkipsEmptyValues(t *testing.T) {
	got := managedPodAnnotations("vk-dev-demo-0", map[string]string{
		meta.AnnotationMode:    "clone",
		meta.AnnotationImage:   "",
		meta.AnnotationStorage: "10G",
	})

	want := map[string]string{
		meta.AnnotationVMName:  "vk-dev-demo-0",
		meta.AnnotationMode:    "clone",
		meta.AnnotationStorage: "10G",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("managedPodAnnotations mismatch:\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestNewManagedPodSharesCommonSkeleton(t *testing.T) {
	cs := &cocoonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "dev", UID: "12345"},
	}

	annotations := managedPodAnnotations("vk-dev-demo-0", map[string]string{
		meta.AnnotationMode: "clone",
	})
	pod := newManagedPod(cs, "demo-0", meta.RoleMain, "demo-image", "cocoon-pool-233", annotations)

	if got := pod.Namespace; got != "dev" {
		t.Fatalf("namespace mismatch: got %q", got)
	}
	if got := pod.Spec.NodeName; got != "cocoon-pool-233" {
		t.Fatalf("node name mismatch: got %q", got)
	}
	if got := pod.Labels[meta.LabelCocoonSet]; got != "demo" {
		t.Fatalf("cocoonset label mismatch: got %q", got)
	}
	if got := pod.Labels[meta.LabelRole]; got != meta.RoleMain {
		t.Fatalf("role label mismatch: got %q", got)
	}
	if got := pod.Labels["app"]; got != "demo" {
		t.Fatalf("app label mismatch: got %q", got)
	}
	if got := pod.Annotations[meta.AnnotationVMName]; got != "vk-dev-demo-0" {
		t.Fatalf("vm name annotation mismatch: got %q", got)
	}
	if got := pod.Spec.Containers; len(got) != 1 || got[0].Name != "vm" || got[0].Image != "demo-image" {
		t.Fatalf("container skeleton mismatch: %#v", got)
	}
	if got := pod.Spec.Tolerations; len(got) != 1 || got[0].Key != meta.TolerationKey {
		t.Fatalf("tolerations mismatch: %#v", got)
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Kind != meta.KindCocoonSet || pod.OwnerReferences[0].Name != "demo" {
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
