# cocoon-operator

Kubernetes operator that manages VM-backed pod lifecycles through two CRDs:
**CocoonSet** (declarative agent groups) and **CocoonHibernation**
(per-pod hibernate / wake requests).

Both reconcilers are built on
[controller-runtime](https://sigs.k8s.io/controller-runtime) and consume
the typed CRD shapes shipped from
[cocoon-common/apis/v1](https://github.com/cocoonstack/cocoon-common).

**Documentation: [cocoonstack.github.io/cocoon-operator](https://cocoonstack.github.io/cocoon-operator/)** (source in [`docs/`](docs/)).

```
cocoon-operator/
├── main.go              # manager wiring + flag parsing
├── cocoonset/           # CocoonSet reconciler, pod builders, status diff
├── hibernation/         # CocoonHibernation reconciler
└── snapshot/            # snapshot.Registry interface consumed by both reconcilers
```

## Quick start

```bash
kubectl apply -k github.com/cocoonstack/cocoon-operator/config/default?ref=main
kubectl -n cocoon-system set env deploy/cocoon-operator \
  OCI_REGISTRY=REGION-docker.pkg.dev/PROJECT/REPO
```

Full steps, including the ADC-less `sa-key` overlay, in
[Installation](docs/installation.md).

## Documentation

- [Architecture](docs/architecture.md) — component diagram, package layout
- [CocoonSet reconcile loop](docs/cocoonset.md) — finalizer/GC, lifecycle-bridge stamp, failed-state and suspend short-circuits, cross-node migration, agent + toolbox reconciliation
- [CocoonHibernation reconcile loop](docs/hibernation.md) — Hibernate/Wake desire handling, finalizer, recoverable failure phases
- [Observability](docs/observability.md) — K8s Events and Prometheus metrics
- [Configuration](docs/configuration.md) — every environment variable
- [Installation](docs/installation.md) — kustomize install, ADC vs SA-key auth, keeping CRDs in sync with cocoon-common

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
