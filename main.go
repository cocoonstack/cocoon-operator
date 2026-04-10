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
	"os"

	"github.com/projecteru2/core/log"

	commonlog "github.com/cocoonstack/cocoon-common/log"
	"github.com/cocoonstack/cocoon-operator/version"
)

func main() {
	ctx := context.Background()
	commonlog.Setup(ctx, "OPERATOR_LOG_LEVEL")
	logger := log.WithFunc("main")

	logger.Infof(ctx, "cocoon-operator %s started (rev=%s built=%s)",
		version.VERSION, version.REVISION, version.BUILTAT)

	// Subsequent commits replace this stub with a controller-runtime
	// Manager that registers the CocoonSet and CocoonHibernation
	// reconcilers and blocks on Manager.Start.
	<-ctx.Done()
	os.Exit(0)
}
