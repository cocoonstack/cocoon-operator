// cocoon-operator runs the CocoonSet and CocoonHibernation controllers.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-logr/logr"
	"github.com/projecteru2/core/log"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
	commonk8s "github.com/cocoonstack/cocoon-common/k8s"
	commonlog "github.com/cocoonstack/cocoon-common/log"
	"github.com/cocoonstack/cocoon-operator/cocoonset"
	"github.com/cocoonstack/cocoon-operator/epoch"
	"github.com/cocoonstack/cocoon-operator/hibernation"
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
	commonlog.Setup(ctx, "OPERATOR_LOG_LEVEL")
	// Silence controller-runtime's own logger; we use core/log instead.
	crlog.SetLogger(logr.Discard())
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

	epochClient := epoch.New(commonk8s.EnvOrDefault("EPOCH_URL", "http://epoch.cocoon-system.svc:8080"), os.Getenv("EPOCH_TOKEN"))

	if err := (&cocoonset.Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Epoch:  epochClient,
	}).SetupWithManager(mgr); err != nil {
		logger.Fatalf(ctx, err, "register cocoonset.Reconciler: %v", err)
	}
	if err := (&hibernation.Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Epoch:  epochClient,
	}).SetupWithManager(mgr); err != nil {
		logger.Fatalf(ctx, err, "register hibernation.Reconciler: %v", err)
	}

	signalCtx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Infof(signalCtx, "starting controller manager (metrics=%s probe=%s leader=%t)",
		metricsAddr, probeAddr, enableLeaderElection)
	if err := mgr.Start(signalCtx); err != nil {
		logger.Fatalf(signalCtx, err, "manager exited with error: %v", err)
	}
}

func buildScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cocoonv1.AddToScheme(scheme))
	return scheme
}
