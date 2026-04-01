package k8sutil

import (
	"os"
	"path/filepath"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// LoadConfig returns a Kubernetes client config using the standard
// fallback chain: KUBECONFIG env -> ~/.kube/config -> in-cluster.
func LoadConfig() (*rest.Config, error) {
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if home, err := os.UserHomeDir(); err == nil {
		kubeconfig := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(kubeconfig); err == nil {
			return clientcmd.BuildConfigFromFlags("", kubeconfig)
		}
	}
	return rest.InClusterConfig()
}
