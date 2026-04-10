// cocoon-operator runs the CocoonSet and CocoonHibernation controllers.
//
// CocoonSet manages a group of VM-backed pods (one main agent + N
// sub-agents + M toolboxes); CocoonHibernation drives per-pod
// hibernate / wake transitions through vk-cocoon. Both reconcilers
// are built on controller-runtime and consume the typed CRD shapes
// shipped from cocoon-common/apis/v1alpha1.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/projecteru2/core/log"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cocoonv1alpha1 "github.com/cocoonstack/cocoon-common/apis/v1alpha1"
	commonlog "github.com/cocoonstack/cocoon-common/log"
	"github.com/cocoonstack/cocoon-operator/version"
)

const (
	defaultMetricsAddr = ":8080"
	defaultProbeAddr   = ":8081"
	leaderElectionID   = "cocoon-operator.cocoonset.cocoonstack.io"
)

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", envOrDefault("METRICS_ADDR", defaultMetricsAddr), "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", envOrDefault("PROBE_ADDR", defaultProbeAddr), "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", envBool("LEADER_ELECT", true), "Enable leader election so only one operator instance reconciles at a time.")
	flag.Parse()

	ctx := context.Background()
	commonlog.Setup(ctx, "OPERATOR_LOG_LEVEL")
	logger := log.WithFunc("main")

	logger.Infof(ctx, "cocoon-operator %s starting (rev=%s built=%s)",
		version.VERSION, version.REVISION, version.BUILTAT)

	scheme := buildScheme()

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       leaderElectionID,
	})
	if err != nil {
		logger.Fatalf(ctx, err, "create manager: %v", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Fatalf(ctx, err, "add healthz check: %v", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		logger.Fatalf(ctx, err, "add readyz check: %v", err)
	}

	epochClient := newEpochClient(envOrDefault("EPOCH_URL", "http://epoch.cocoon-system.svc:8080"), os.Getenv("EPOCH_TOKEN"))

	if err := (&CocoonSetReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Epoch:  epochClient,
	}).SetupWithManager(mgr); err != nil {
		logger.Fatalf(ctx, err, "register CocoonSetReconciler: %v", err)
	}
	if err := (&CocoonHibernationReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Epoch:  epochClient,
	}).SetupWithManager(mgr); err != nil {
		logger.Fatalf(ctx, err, "register CocoonHibernationReconciler: %v", err)
	}

	signalCtx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Infof(signalCtx, "starting controller manager (metrics=%s probe=%s leader=%t)",
		metricsAddr, probeAddr, enableLeaderElection)
	if err := mgr.Start(signalCtx); err != nil {
		logger.Fatalf(signalCtx, err, "manager exited with error: %v", err)
	}
}

// buildScheme returns a runtime.Scheme with the core kubernetes
// types and the cocoon-common CRD types registered.
func buildScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cocoonv1alpha1.AddToScheme(scheme))
	return scheme
}

// envOrDefault returns the value of key from the environment, falling
// back to fallback when key is unset or empty.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envBool reads a boolean env var. Recognized truthy values are
// "1", "true", "yes" (case-insensitive). Anything else is false.
func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "TRUE", "True", "yes", "YES", "Yes":
		return true
	default:
		return false
	}
}
