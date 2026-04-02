// Package cocoonmeta defines shared metadata keys and naming rules used across
// Cocoon controllers, webhooks, dashboards, and providers.
package cocoonmeta

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	APIVersion    = "cocoon.cis/v1alpha1"
	KindCocoonSet = "CocoonSet"

	TolerationKey = "virtual-kubelet.io/provider"

	LabelCocoonSet = "cocoon.cis/cocoonset"
	LabelRole      = "cocoon.cis/role"
	LabelSlot      = "cocoon.cis/slot"

	AnnotationMode           = "cocoon.cis/mode"
	AnnotationImage          = "cocoon.cis/image"
	AnnotationStorage        = "cocoon.cis/storage"
	AnnotationManaged        = "cocoon.cis/managed"
	AnnotationOS             = "cocoon.cis/os"
	AnnotationForkFrom       = "cocoon.cis/fork-from"
	AnnotationSnapshotPolicy = "cocoon.cis/snapshot-policy"
	AnnotationIP             = "cocoon.cis/ip"
	AnnotationVMID           = "cocoon.cis/vm-id"
	AnnotationVMName         = "cocoon.cis/vm-name"
	AnnotationVNCPort        = "cocoon.cis/vnc-port"
	AnnotationHibernate      = "cocoon.cis/hibernate"

	RoleMain     = "main"
	RoleSubAgent = "sub-agent"
	RoleToolbox  = "toolbox"
)

func HasCocoonToleration(tolerations []corev1.Toleration) bool {
	for _, tol := range tolerations {
		if tol.Key == TolerationKey {
			return true
		}
	}
	return false
}

func DeploymentNameFromOwnerRefs(ownerRefs []metav1.OwnerReference) string {
	for _, ref := range ownerRefs {
		if ref.Kind != "ReplicaSet" {
			continue
		}
		parts := strings.Split(ref.Name, "-")
		if len(parts) >= 2 {
			return strings.Join(parts[:len(parts)-1], "-")
		}
	}
	return ""
}

func VMNameForDeployment(namespace, deployment string, slot int) string {
	return fmt.Sprintf("vk-%s-%s-%d", namespace, deployment, slot)
}

func VMNameForPod(namespace, podName string) string {
	return fmt.Sprintf("vk-%s-%s", namespace, podName)
}

func ExtractSlotFromVMName(vmName string) int {
	idx := strings.LastIndex(vmName, "-")
	if idx < 0 {
		return -1
	}
	n, err := strconv.Atoi(vmName[idx+1:])
	if err != nil {
		return -1
	}
	return n
}

func MainAgentVMName(vmName string) string {
	idx := strings.LastIndex(vmName, "-")
	if idx < 0 {
		return vmName
	}
	return vmName[:idx] + "-0"
}

func InferRoleFromVMName(vmName string) string {
	if ExtractSlotFromVMName(vmName) == 0 {
		return RoleMain
	}
	return RoleSubAgent
}
