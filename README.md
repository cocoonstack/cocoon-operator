# cocoon-operator

Kubernetes operator that manages VM-backed pod lifecycles through two CRDs:

- **CocoonSet** — declarative spec for an agent group (one main agent + N sub-agents + M toolboxes)
- **CocoonHibernation** — per-pod hibernate / wake request

Both reconcilers are built on [controller-runtime](https://sigs.k8s.io/controller-runtime) and consume the typed CRD shapes shipped from [cocoon-common/apis/v1](https://github.com/cocoonstack/cocoon-common).

The binary entry point is `main.go`; the reconcilers themselves live in subpackages so each one is independently testable:

```
cocoon-operator/
├── main.go              # manager wiring + flag parsing
├── cocoonset/           # CocoonSet reconciler, pod builders, status diff
├── hibernation/         # CocoonHibernation reconciler
└── epoch/               # SnapshotRegistry interface + epoch HTTP adapter
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        cocoon-operator                            │
│                                                                  │
│  ┌────────────────────────┐    ┌─────────────────────────────┐  │
│  │  cocoonset.Reconciler  │    │ hibernation.Reconciler      │  │
│  │  - finalizer + GC       │    │  - HibernateState patches   │  │
│  │  - main → subs → tbs    │    │  - epoch.HasManifest probe  │  │
│  │  - patch /status        │    │  - Conditions               │  │
│  └────────┬───────────────┘    └────────────┬────────────────┘  │
│           │                                  │                   │
│           ▼                                  ▼                   │
│  ┌────────────────────┐         ┌──────────────────────┐        │
│  │ controller-runtime │         │ epoch SnapshotRegistry│        │
│  │ Manager            │         │ (HTTP via             │        │
│  │  - leader election │         │  registryclient)      │        │
│  │  - metrics :8080    │        └──────────────────────┘        │
│  │  - probes :8081     │                                         │
│  └────────────────────┘                                          │
└──────────────────────────────────────────────────────────────────┘
```

### CocoonSet reconcile loop

1. Fetch the CocoonSet (return early on NotFound).
2. If `DeletionTimestamp` is set, walk owned pods, delete them, optionally `epoch.DeleteManifest` for each VM, then drop the finalizer.
3. Ensure the `cocoonset.cocoonstack.io/finalizer` is in place.
4. List owned pods by `cocoonset.cocoonstack.io/name=<cs.Name>` and classify by role label.
5. **Suspend short-circuit**: if `spec.suspend == true`, write `meta.HibernateState(true)` onto every pod and report `Phase=Suspended`.
6. Ensure the **main agent** (slot 0). If it is not yet `Ready`, requeue in 5 seconds and report `Phase=Pending`.
7. Ensure sub-agents `[1..Replicas]`; delete extras above the requested count.
8. Ensure toolboxes by name; delete extras.
9. Re-list and patch `/status` (with structural diff so unchanged status patches are no-ops).

Pods are constructed via `meta.VMSpec.Apply` so the operator never touches the annotation map directly. The `For` watch uses `predicate.GenerationChangedPredicate` so reconciles only fire when the spec actually changes — status-only patches the operator makes itself never loop back. The `Owns` side keeps the unfiltered pod-event firehose because pod status changes are exactly what drives the readyAgents diff.

### CocoonHibernation reconcile loop

| Spec.Desire | What the reconciler does | Terminal phase |
|---|---|---|
| `Hibernate` | `meta.HibernateState(true).Apply` on the target pod, then poll `epoch.HasManifest(vmName, "hibernate")` until the snapshot lands | `Hibernated` |
| `Wake` | Clear `meta.HibernateState`, wait for the pod's container to be `Running`, then drop the hibernation snapshot tag from epoch | `Active` |

There is no `cocoon-vm-snapshots` ConfigMap bridge — epoch is the single source of truth for hibernation state. Failure paths set `Phase=Failed` with a one-shot message in the `Ready` condition instead of looping forever on a bad reference.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KUBECONFIG` | unset | Path to kubeconfig when running outside the cluster |
| `OPERATOR_LOG_LEVEL` | `info` | `projecteru2/core/log` level |
| `EPOCH_URL` | `http://epoch.cocoon-system.svc:8080` | Base URL of the epoch registry |
| `EPOCH_TOKEN` | unset | Bearer token (read-only is enough) |
| `METRICS_ADDR` | `:8080` | Prometheus listener |
| `PROBE_ADDR` | `:8081` | healthz / readyz listener |
| `LEADER_ELECT` | `true` | Enable leader election so only one replica reconciles |

CLI flags (`--metrics-bind-address`, `--health-probe-bind-address`, `--leader-elect`) override the corresponding env var.

## Installation

```bash
kubectl apply -k github.com/cocoonstack/cocoon-operator/config/default?ref=main
```

This installs:
- `cocoon-system` namespace
- Both CRDs (imported from `cocoon-common` via `make import-crds`)
- `ServiceAccount`, `ClusterRole`, and `ClusterRoleBinding`
- The operator `Deployment` (1 replica with leader election on)

To override the image tag or replica count, build a kustomize overlay that imports `config/default` as a base.

### Keeping CRDs in sync with cocoon-common

The CRD YAML lives under `config/crd/bases/` and is committed so a clean clone works out of the box. After bumping the cocoon-common dependency, regenerate the bases with:

```bash
go get github.com/cocoonstack/cocoon-common@<version>
make import-crds
git add config/crd/bases && git commit
```

The `import-crds` target uses `go list -m -f '{{.Dir}}'` to resolve the cocoon-common module path and copies the YAML straight from there. CI rejects PRs that forget this step.

## Development

```bash
make all            # full pipeline: deps + fmt + lint + test + build
make build          # build cocoon-operator binary
make test           # vet + race-detected tests
make lint           # golangci-lint on linux + darwin
make import-crds    # refresh config/crd/bases from cocoon-common
make help           # show all targets
```

The Makefile detects Go workspace mode (`go env GOWORK`) and skips `go mod tidy` when active so cross-module references resolve through `go.work` without forcing a release of cocoon-common.

## Related projects

| Project | Role |
|---|---|
| [cocoon-common](https://github.com/cocoonstack/cocoon-common) | CRD types, annotation contract, shared helpers |
| [cocoon-webhook](https://github.com/cocoonstack/cocoon-webhook) | Admission webhook for sticky scheduling and CocoonSet validation |
| [epoch](https://github.com/cocoonstack/epoch) | Snapshot registry; the operator queries it via `SnapshotRegistry` |
| [vk-cocoon](https://github.com/cocoonstack/vk-cocoon) | Virtual kubelet provider managing VM lifecycle |

## License

[MIT](LICENSE)
