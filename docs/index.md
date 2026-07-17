# cocoon-operator

Kubernetes operator that manages VM-backed pod lifecycles through two CRDs:
CocoonSet (declarative agent groups) and CocoonHibernation (per-pod
hibernate / wake requests). Both reconcilers are built on
[controller-runtime](https://sigs.k8s.io/controller-runtime).

```
CocoonSet          ──► cocoonset.Reconciler    ──► main + sub-agent + toolbox pods
CocoonHibernation  ──► hibernation.Reconciler  ──► HibernateState patch + registry manifest probe
                                                ──► snapshot.Registry (OCI, via registryclient)
```

## Guides

- [Architecture](architecture.md) — component diagram and package layout
- [CocoonSet reconcile loop](cocoonset.md) — finalizer/GC, lifecycle-bridge
  stamp, failed-state and suspend short-circuits, cross-node migration,
  main/sub-agent/toolbox reconciliation
- [CocoonHibernation reconcile loop](hibernation.md) — Hibernate/Wake
  desire handling, finalizer, recoverable failure phases
- [Observability](observability.md) — K8s Event reasons and Prometheus
  metrics
- [Configuration](configuration.md) — every environment variable
- [Installation](installation.md) — kustomize install, ADC vs SA-key auth,
  keeping CRDs in sync with cocoon-common

## Repository

Source and issue tracker:
[github.com/cocoonstack/cocoon-operator](https://github.com/cocoonstack/cocoon-operator).
Part of the [cocoonstack](https://cocoonstack.github.io/) MicroVM platform.
