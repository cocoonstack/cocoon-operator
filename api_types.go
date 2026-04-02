package main

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type hibernation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              hibernationSpec   `json:"spec,omitempty"`
	Status            hibernationStatus `json:"status,omitempty"`
}

type hibernationSpec struct {
	PodName string `json:"podName,omitempty"`
	Action  string `json:"action,omitempty"`
}

type hibernationStatus struct {
	Phase              string `json:"phase,omitempty"`
	Message            string `json:"message,omitempty"`
	VMName             string `json:"vmName,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

type cocoonSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              cocoonSetSpec   `json:"spec,omitempty"`
	Status            cocoonSetStatus `json:"status,omitempty"`
}

type cocoonSetSpec struct {
	Suspend        bool                `json:"suspend,omitempty"`
	SnapshotPolicy string              `json:"snapshotPolicy,omitempty"`
	NodeName       string              `json:"nodeName,omitempty"`
	Agent          cocoonSetAgentSpec  `json:"agent,omitempty"`
	Toolboxes      []cocoonToolboxSpec `json:"toolboxes,omitempty"`
}

type cocoonSetAgentSpec struct {
	Replicas           int64               `json:"replicas,omitempty"`
	Image              string              `json:"image,omitempty"`
	Storage            string              `json:"storage,omitempty"`
	ServiceAccountName string              `json:"serviceAccountName,omitempty"`
	Resources          resourceHints       `json:"resources,omitempty"`
	EnvFrom            []envFromSourceSpec `json:"envFrom,omitempty"`
}

type cocoonToolboxSpec struct {
	Name       string        `json:"name,omitempty"`
	OS         string        `json:"os,omitempty"`
	Image      string        `json:"image,omitempty"`
	Mode       string        `json:"mode,omitempty"`
	Storage    string        `json:"storage,omitempty"`
	StaticIP   string        `json:"staticIP,omitempty"`
	StaticVMID string        `json:"staticVMID,omitempty"`
	VNCPort    int64         `json:"vncPort,omitempty"`
	Resources  resourceHints `json:"resources,omitempty"`
}

type resourceHints struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

type envFromSourceSpec struct {
	ConfigMapRef *namedObjectReference `json:"configMapRef,omitempty"`
	SecretRef    *namedObjectReference `json:"secretRef,omitempty"`
}

type namedObjectReference struct {
	Name string `json:"name,omitempty"`
}

type cocoonSetStatus struct {
	Phase         string                   `json:"phase,omitempty"`
	ReadyAgents   int64                    `json:"readyAgents,omitempty"`
	DesiredAgents int64                    `json:"desiredAgents,omitempty"`
	Agents        []cocoonSetAgentStatus   `json:"agents,omitempty"`
	Toolboxes     []cocoonSetToolboxStatus `json:"toolboxes,omitempty"`
}

type cocoonSetAgentStatus struct {
	Slot       int64  `json:"slot,omitempty"`
	Role       string `json:"role,omitempty"`
	PodName    string `json:"podName,omitempty"`
	VMName     string `json:"vmName,omitempty"`
	Phase      string `json:"phase,omitempty"`
	VMID       string `json:"vmID,omitempty"`
	IP         string `json:"ip,omitempty"`
	ForkedFrom string `json:"forkedFrom,omitempty"`
}

type cocoonSetToolboxStatus struct {
	Name     string `json:"name,omitempty"`
	PodName  string `json:"podName,omitempty"`
	VMName   string `json:"vmName,omitempty"`
	Phase    string `json:"phase,omitempty"`
	IP       string `json:"ip,omitempty"`
	VMID     string `json:"vmID,omitempty"`
	ConnType string `json:"connType,omitempty"`
	VNCPort  int64  `json:"vncPort,omitempty"`
}

func decodeUnstructured[T any](u *unstructured.Unstructured) (*T, error) {
	var out T
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &out); err != nil {
		return nil, fmt.Errorf("decode %T: %w", out, err)
	}
	return &out, nil
}

func (s cocoonSetSpec) targetNodeName() string {
	if s.NodeName != "" {
		return s.NodeName
	}
	return "cocoon-pool"
}

func (s cocoonSetSpec) snapshotPolicy() string {
	if s.SnapshotPolicy != "" {
		return s.SnapshotPolicy
	}
	return "always"
}

func (t cocoonToolboxSpec) osType() string {
	if t.OS != "" {
		return t.OS
	}
	return "linux"
}

func (t cocoonToolboxSpec) mode() string {
	if t.Mode != "" {
		return t.Mode
	}
	return "run"
}

func (s cocoonSetStatus) toolboxRuntimeHints(name string) *cocoonSetToolboxStatus {
	for i := range s.Toolboxes {
		if s.Toolboxes[i].Name == name {
			return &s.Toolboxes[i]
		}
	}
	return nil
}

func envFromSource(specs []envFromSourceSpec) []corev1.EnvFromSource {
	out := make([]corev1.EnvFromSource, 0, len(specs))
	for _, item := range specs {
		if item.ConfigMapRef != nil && item.ConfigMapRef.Name != "" {
			out = append(out, corev1.EnvFromSource{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: item.ConfigMapRef.Name},
				},
			})
		}
		if item.SecretRef != nil && item.SecretRef.Name != "" {
			out = append(out, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: item.SecretRef.Name},
				},
			})
		}
	}
	return out
}
