package cocoonmeta

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestVMNamingHelpers(t *testing.T) {
	if got := VMNameForDeployment("prod", "demo", 2); got != "vk-prod-demo-2" {
		t.Fatalf("deployment vm name mismatch: got %q", got)
	}
	if got := VMNameForPod("prod", "toolbox"); got != "vk-prod-toolbox" {
		t.Fatalf("pod vm name mismatch: got %q", got)
	}
	if got := ExtractSlotFromVMName("vk-prod-demo-2"); got != 2 {
		t.Fatalf("slot mismatch: got %d", got)
	}
	if got := ExtractSlotFromVMName("vk-prod-toolbox"); got != -1 {
		t.Fatalf("expected non-slot vm name to return -1, got %d", got)
	}
	if got := MainAgentVMName("vk-prod-demo-2"); got != "vk-prod-demo-0" {
		t.Fatalf("main agent name mismatch: got %q", got)
	}
}

func TestInferRoleFromVMName(t *testing.T) {
	if got := InferRoleFromVMName("vk-prod-demo-0"); got != RoleMain {
		t.Fatalf("expected role %q, got %q", RoleMain, got)
	}
	if got := InferRoleFromVMName("vk-prod-demo-3"); got != RoleSubAgent {
		t.Fatalf("expected role %q, got %q", RoleSubAgent, got)
	}
}

func TestConnectionType(t *testing.T) {
	cases := []struct {
		name       string
		osType     string
		hasVNCPort bool
		want       string
	}{
		{name: "vnc wins", osType: "windows", hasVNCPort: true, want: "vnc"},
		{name: "windows", osType: "windows", want: "rdp"},
		{name: "android", osType: "android", want: "adb"},
		{name: "default", osType: "linux", want: "ssh"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ConnectionType(tc.osType, tc.hasVNCPort); got != tc.want {
				t.Fatalf("connection type mismatch: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestDeploymentNameFromOwnerRefs(t *testing.T) {
	ownerRefs := []metav1.OwnerReference{
		{Kind: "ReplicaSet", Name: "demo-7b7c9d9d5f"},
	}
	if got := DeploymentNameFromOwnerRefs(ownerRefs); got != "demo" {
		t.Fatalf("deployment name mismatch: got %q", got)
	}
}

func TestHasCocoonToleration(t *testing.T) {
	tolerations := []corev1.Toleration{{Key: TolerationKey}}
	if !HasCocoonToleration(tolerations) {
		t.Fatalf("expected toleration to be detected")
	}
}
