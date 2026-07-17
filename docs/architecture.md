# Architecture

cocoon-operator manages VM-backed pod lifecycles through two CRDs:

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

## Component diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                        cocoon-operator                            │
│                                                                  │
│  ┌────────────────────────┐    ┌─────────────────────────────┐  │
│  │  cocoonset.Reconciler  │    │ hibernation.Reconciler      │  │
│  │  - finalizer + GC       │    │  - HibernateState patches   │  │
│  │  - migration (nodeName) │    │  - registry manifest probe  │  │
│  │  - main → subs → tbs    │    │  - Conditions               │  │
│  │  - patch /status        │    │                             │  │
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

See [CocoonSet reconcile loop](cocoonset.md) and
[CocoonHibernation reconcile loop](hibernation.md) for the two reconcilers
in detail, and [Observability](observability.md) for how failures surface
as Events and metrics.
