package main

import (
	"cmp"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	modeClone  = "clone"
	modeRun    = "run"
	modeStatic = "static"

	osLinux   = "linux"
	osWindows = "windows"

	defaultNodeName       = "cocoon-pool"
	defaultSnapshotPolicy = "always"
)

type hibernation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              hibernationSpec   `json:"spec"`
	Status            hibernationStatus `json:"status"`
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
	metav1.ObjectMeta `json:"metadata"`
	Spec              cocoonSetSpec   `json:"spec"`
	Status            cocoonSetStatus `json:"status"`
}

type cocoonSetSpec struct {
	Suspend        bool                `json:"suspend,omitempty"`
	SnapshotPolicy string              `json:"snapshotPolicy,omitempty"`
	NodeName       string              `json:"nodeName,omitempty"`
	Agent          cocoonSetAgentSpec  `json:"agent"`
	Toolboxes      []cocoonToolboxSpec `json:"toolboxes,omitempty"`
}

type cocoonSetAgentSpec struct {
	Replicas           int64               `json:"replicas,omitempty"`
	Image              string              `json:"image,omitempty"`
	Mode               string              `json:"mode,omitempty"`
	OS                 string              `json:"os,omitempty"`
	Network            string              `json:"network,omitempty"`
	Storage            string              `json:"storage,omitempty"`
	ServiceAccountName string              `json:"serviceAccountName,omitempty"`
	Resources          resourceHints       `json:"resources"`
	EnvFrom            []envFromSourceSpec `json:"envFrom,omitempty"`
}

func (a cocoonSetAgentSpec) osType() string {
	return cmp.Or(a.OS, osLinux)
}

func (a cocoonSetAgentSpec) modeType() string {
	return cmp.Or(a.Mode, modeClone)
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
	Resources  resourceHints `json:"resources"`
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

func (s cocoonSetSpec) targetNodeName() string {
	return cmp.Or(s.NodeName, defaultNodeName)
}

func (s cocoonSetSpec) snapshotPolicy() string {
	return cmp.Or(s.SnapshotPolicy, defaultSnapshotPolicy)
}

func (t cocoonToolboxSpec) osType() string {
	return cmp.Or(t.OS, osLinux)
}

func (t cocoonToolboxSpec) mode() string {
	return cmp.Or(t.Mode, modeRun)
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
