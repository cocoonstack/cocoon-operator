# Configuration

| Variable | Default | Description |
|---|---|---|
| `KUBECONFIG` | unset | Path to kubeconfig when running outside the cluster |
| `OPERATOR_LOG_LEVEL` | `info` | `projecteru2/core/log` level |
| `OCI_REGISTRY` | **required** | OCI registry base for snapshot manifests (e.g. an Artifact Registry repo). Auth resolves GCP ADC then docker config. |
| `METRICS_ADDR` | `:8080` | Prometheus listener |
| `PROBE_ADDR` | `:8081` | healthz / readyz listener |
| `LEADER_ELECT` | `true` | Enable leader election so only one replica reconciles |
| `COCOONSET_CONCURRENCY` | `4` | Maximum concurrent CocoonSet reconciles. Must be at least 1. |
| `HIBERNATION_CONCURRENCY` | `4` | Maximum concurrent CocoonHibernation reconciles. Must be at least 1. |

CLI flags (`--metrics-bind-address`, `--health-probe-bind-address`, `--leader-elect`, `--cocoonset-concurrency`, `--hibernation-concurrency`) override the corresponding env var.

Reconciles block on registry round trips, so concurrency above 1 overlaps those waits across unrelated resources. Reconciles of one resource are never concurrent, and CocoonHibernation CRs that target the same pod are serialized against each other.
