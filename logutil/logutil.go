// Package logutil provides a shared log initialization function for cocoonstack projects.
package logutil

import (
	"context"
	"os"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/types"
)

// Setup initializes the core/log logger with the level from the named
// environment variable, defaulting to "info". Fatals on failure.
func Setup(ctx context.Context, envVar string) {
	level := os.Getenv(envVar)
	if level == "" {
		level = "info"
	}
	if err := log.SetupLog(ctx, &types.ServerLogConfig{Level: level}, ""); err != nil {
		log.WithFunc("logutil.Setup").Fatalf(ctx, err, "setup log: %v", err)
	}
}
