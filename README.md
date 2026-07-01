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
└── snapshot/            # snapshot.Registry interface consumed by both reconcilers
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        cocoon-operator                            │
│                                                                  │
│  ┌────────────────────────┐    ┌─────────────────────────────┐  │
│  │  cocoonset.Reconciler  │    │ hibernation.Reconciler      │  │
│  │  - finalizer + GC       │    │  - HibernateState patches   │  │
│  │  - main → subs → tbs    │    │  - registry manifest probe  │  │
│  │  - patch /status        │    │  - Conditions               │  │
│  └────────┬───────────────┘    └────────────┬────────────────┘  │
│           │                                  │                   │
│           ▼                                  ▼                   │
│  ┌────────────────────┐         ┌──────────────────────┐        │
│  │ controller-runtime │         │ snapshot.Registry    │        │
│  │ Manager            │         │ (HTTP via            │        │
│  │  - leader election │         │  registryclient)     │        │
│  │  - metrics :8080   │         └──────────────────────┘        │
│  │  - probes :8081    │                                          │
│  └────────────────────┘                                          │
└──────────────────────────────────────────────────────────────────┘
```

### CocoonSet reconcile loop

1. Fetch the CocoonSet (return early on NotFound).
2. If `DeletionTimestamp` is set, walk owned pods, delete them, `Registry.DeleteManifest` for both `:latest` and `:hibernate` tags on every owned VM (unconditional — DeleteManifest is 404-tolerant, and hibernate pushes ignore snapshotPolicy so any main-only gate would orphan `:hibernate` tags pushed by sub-agents), then drop the finalizer. VM names to GC are stashed onto an annotation before pod deletion so the cleanup survives a CocoonSet deleted before `Status.Agents` was ever patched.
3. Ensure the `cocoonset.cocoonstack.io/finalizer` is in place.
4. List owned pods by `cocoonset.cocoonstack.io/name=<cs.Name>`, drop any with stale labels that aren't actually controller-owned, and classify the rest by role label.
5. **Lifecycle-bridge stamp**: patch `cs.Generation` onto each owned pod's `cocoonset.cocoonstack.io/generation` annotation so vk-cocoon can echo it back as `lifecycle-observed-generation`, giving clients a counter-based completion signal immune to wallclock skew.
6. **Failed-state short-circuit**: if the main pod is terminal (`Pod.Phase=Failed`, or it carries `vm.cocoonstack.io/lifecycle-state=Failed` from vk-cocoon while still Running), patch `Phase=Failed` and emit `MainAgentFailed` / `PodLifecycleFailed`. The Failed phase is recoverable: when the main pod becomes `Ready` again the operator emits `RecoveredFromFailure` and resumes normal reconciliation.
7. **Suspend short-circuit**: if `spec.suspend == true`, write `meta.HibernateState(true)` onto every owned pod and poll the registry for `:hibernate` manifests on every managed VM. Stay in `Phase=Suspending` (requeueing every 5 s) until every required snapshot lands, then transition to `Phase=Suspended`.
8. **Un-suspend**: if `spec.suspend == false` and any owned pod still carries the hibernate annotation from a prior suspend, clear it via `PatchHibernateState(false)` so vk-cocoon wakes the VMs. Pods that are the active target of a `desire=Hibernate` CocoonHibernation CR are skipped to avoid racing the hibernation reconciler. `PatchHibernateState(false)` is a no-op on pods whose annotation is already absent, so this is cheap in the common "never suspended" case.
9. Ensure the **main agent** (slot 0). If the existing pod has drifted from spec, delete it for recreate. If it is not yet `Ready`, requeue in 5 s and report `Phase=Pending`.
10. Ensure sub-agents `[1..Replicas]` (creates are fanned out via an errgroup capped at 8 concurrent pod creates so a large scale-up does not burst the apiserver); delete extras above the requested count.
11. Ensure toolboxes by name; skip creation with an error if the toolbox pod name collides with an existing non-toolbox pod (e.g. an agent). Delete extras.
12. Re-list and patch `/status` (with structural diff so unchanged status patches are no-ops).

Pods are constructed via `meta.FromAgentSpec` / `meta.FromToolboxSpec` factory helpers so the operator never touches the annotation map directly. These factories propagate the full `VMOptions` surface (OS, Backend, ConnType, Network, ForcePull, NoDirectIO, ProbePort, Storage, Resources) into the pod annotations that vk-cocoon consumes. The `For` watch uses `predicate.GenerationChangedPredicate` so reconciles only fire when the spec actually changes — status-only patches the operator makes itself never loop back. The `Owns` side filters pod events to creation, deletion, and meaningful transitions (phase change, readiness flip, label/annotation mutation) via a `podRelevantChange` predicate so pure VK status churn does not trigger reconcile storms.

### CocoonHibernation reconcile loop

| Spec.Desire | What the reconciler does | Terminal phase |
|---|---|---|
| `Hibernate` | `meta.HibernateState(true).Apply` on the target pod, then poll `Registry.HasManifest(vmName, meta.HibernateSnapshotTag)` until the snapshot lands or `hibernateTimeout` (3 minutes) trips. A probe error (transport / 5xx / auth) surfaces as a returned error so controller-runtime logs + retries with backoff. | `Hibernated` |
| `Wake` | Check if the container is already `Running` (skip annotation patch if so), otherwise clear `meta.HibernateState` **once** (skip if already cleared to avoid triggering informer events on every requeue cycle), then wait for the container to be `Running` and drop the hibernation snapshot tag from the registry. A wake that does not complete within `wakeTimeout` (5 minutes) is escalated to `Phase=Failed` with a dated message in the `Ready` condition. | `Active` |

On CR deletion the reconciler runs a finalizer (`cocoonhibernation.cocoonset.cocoonstack.io/finalizer`) that clears the `:hibernate` tag from the registry (if `Status.VMName` is set) before removing itself, so deleting a CocoonHibernation never leaves an orphaned snapshot on the registry.

There is no `cocoon-vm-snapshots` ConfigMap bridge — the registry is the single source of truth for hibernation state. Failure paths set `Phase=Failed` with a one-shot message in the `Ready` condition instead of looping forever on a bad reference. Both `Hibernate` and `Wake` Failed phases are recoverable: on re-entry from a non-deadline phase the reconciler refreshes the Ready condition's `LastTransitionTime` so the budget resets cleanly (without the override, `apimeta.SetStatusCondition` would preserve the stale timestamp across the `False → False` transition and the recovered phase would trip the deadline on the next reconcile). Each retry emits a `RetryRequested` Normal Event so the recovery is visible in `kubectl describe`.

### Observability

Reconciler failures surface as K8s Events on the CR plus Prometheus counters on the controller-runtime `/metrics` endpoint:

| Event reason (CocoonHibernation) | Type |
|---|---|
| `HibernateTimedOut`, `WakeTimedOut` | Warning |
| `Hibernated`, `WokenActive`, `RetryRequested` | Normal |

| Event reason (CocoonSet) | Type |
|---|---|
| `PodLifecycleFailed`, `MainAgentFailed`, `SubAgentDeadLetter` | Warning |
| `SubAgentRebuilding`, `RecoveredFromFailure` | Normal |

Metrics:

```
cocoon_operator_subagent_rebuild_total{namespace, cocoonset}
cocoon_operator_subagent_dead_letter_total{namespace, cocoonset}
cocoon_operator_hibernate_phase_duration_seconds{result}    # result=ok|timeout
cocoon_operator_wake_phase_duration_seconds{result}
cocoon_operator_lifecycle_state_failed_observed_total{phase}
```

`CocoonSet` consumes the `vm.cocoonstack.io/lifecycle-state=Failed` annotation that vk-cocoon writes on terminal failures (hibernate, wake, post-clone, SAC); the operator treats it as terminal on every owned pod role (main, sub-agent, toolbox) so reconciliation reacts immediately instead of waiting for `Pod.Status.Phase` to follow. `triageSubAgent` rebuilds a terminal sub pod up to four times with `0/1/5/30 s` exponential backoff between attempts, then marks the pod `cocoonset.cocoonstack.io/dead-letter=true` and leaves it in place so a permanently broken slot stops consuming the apiserver budget. Rebuild count persists in the `cocoonset.cocoonstack.io/rebuild-history` annotation on the CocoonSet so the count survives the pod delete; entries for slots beyond the current `spec.agent.replicas` are garbage-collected on every write.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KUBECONFIG` | unset | Path to kubeconfig when running outside the cluster |
| `OPERATOR_LOG_LEVEL` | `info` | `projecteru2/core/log` level |
| `OCI_REGISTRY` | **required** | OCI registry base for snapshot manifests (e.g. an Artifact Registry repo). Auth resolves GCP ADC then docker config. |
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
| [cocoon-common](https://github.com/cocoonstack/cocoon-common) | CRD types, annotation contract, shared helpers, and the OCI registry client |
| [cocoon-webhook](https://github.com/cocoonstack/cocoon-webhook) | Admission webhook for sticky scheduling and CocoonSet validation |
| [vk-cocoon](https://github.com/cocoonstack/vk-cocoon) | Virtual kubelet provider managing VM lifecycle |

## License

[MIT](LICENSE)
