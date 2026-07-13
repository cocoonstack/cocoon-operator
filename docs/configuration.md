# Configuration

| Variable | Default | Description |
|---|---|---|
| `KUBECONFIG` | unset | Path to kubeconfig when running outside the cluster |
| `OPERATOR_LOG_LEVEL` | `info` | `projecteru2/core/log` level |
| `OCI_REGISTRY` | **required** | OCI registry base for snapshot manifests (e.g. an Artifact Registry repo). Auth resolves GCP ADC then docker config. |
| `METRICS_ADDR` | `:8080` | Prometheus listener |
| `PROBE_ADDR` | `:8081` | healthz / readyz listener |
| `LEADER_ELECT` | `true` | Enable leader election so only one replica reconciles |

CLI flags (`--metrics-bind-address`, `--health-probe-bind-address`, `--leader-elect`) override the corresponding env var.
