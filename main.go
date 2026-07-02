// Package main is the cocoon-operator entry point. It runs the CocoonSet
// and CocoonHibernation reconcilers under controller-runtime.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	commonlog "github.com/cocoonstack/cocoon-common/log"
	"github.com/cocoonstack/cocoon-common/oci"
	"github.com/cocoonstack/cocoon-operator/cocoonset"
	"github.com/cocoonstack/cocoon-operator/hibernation"
	"github.com/cocoonstack/cocoon-operator/metrics"
	"github.com/cocoonstack/cocoon-operator/snapshot"
	"github.com/cocoonstack/cocoon-operator/version"
)

const (
	defaultMetricsAddr = ":8080"
	defaultProbeAddr   = ":8081"
	leaderElectionID   = "cocoon-operator.cocoonset.cocoonstack.io"
)

func main() {
	leaderDefault := commonk8s.EnvBool("LEADER_ELECT", true)

	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", commonk8s.EnvOrDefault("METRICS_ADDR", defaultMetricsAddr), "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", commonk8s.EnvOrDefault("PROBE_ADDR", defaultProbeAddr), "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", leaderDefault, "Enable leader election so only one operator instance reconciles at a time.")
	flag.Parse()

	ctx := context.Background()
	if err := commonlog.Setup(ctx, "OPERATOR_LOG_LEVEL"); err != nil {
		fmt.Fprintf(os.Stderr, "setup log: %v\n", err)
		os.Exit(1)
	}
	// Route controller-runtime's logger through core/log: reconcile errors
	// surface only there, so discarding it hides every one of them.
	crlog.SetLogger(newCRLogger(ctx))
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

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Fatalf(ctx, err, "add healthz check: %v", err)
	}
	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		logger.Fatalf(ctx, err, "add readyz check: %v", err)
	}

	registry, err := buildRegistry()
	if err != nil {
		logger.Fatalf(ctx, err, "create registry client: %v", err)
	}

	metrics.Register()

	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		logger.Fatalf(ctx, err, "build clientset: %v", err)
	}
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientset.CoreV1().Events("")})
	defer broadcaster.Shutdown()
	recorder := broadcaster.NewRecorder(mgr.GetScheme(), corev1.EventSource{Component: "cocoon-operator"})

	if err = (&cocoonset.Reconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Registry: registry,
		Recorder: recorder,
	}).SetupWithManager(ctx, mgr); err != nil {
		logger.Fatalf(ctx, err, "register cocoonset.Reconciler: %v", err)
	}
	if err = (&hibernation.Reconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Registry: registry,
		Recorder: recorder,
	}).SetupWithManager(ctx, mgr); err != nil {
		logger.Fatalf(ctx, err, "register hibernation.Reconciler: %v", err)
	}

	signalCtx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Infof(signalCtx, "starting controller manager (metrics=%s probe=%s leader=%t)",
		metricsAddr, probeAddr, enableLeaderElection)
	if err = mgr.Start(signalCtx); err != nil {
		logger.Fatalf(signalCtx, err, "manager exited with error: %v", err)
	}
}

// buildRegistry builds the OCI registry backend from OCI_REGISTRY. The keychain
// resolves GCP ADC (google.Keychain) then docker config.
func buildRegistry() (snapshot.Registry, error) {
	base := os.Getenv("OCI_REGISTRY")
	if base == "" {
		return nil, errors.New("OCI_REGISTRY must be set")
	}
	keychain := authn.NewMultiKeychain(google.Keychain, authn.DefaultKeychain)
	return oci.NewOCIRegistry(base, keychain), nil
}

func buildScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cocoonv1.AddToScheme(scheme))
	return scheme
}
