# Installation

The operator requires [cocoon-webhook](https://github.com/cocoonstack/cocoon-webhook)
in the cluster: its admission validator enforces one CocoonHibernation per pod
(`metadata.name == spec.podRef.name`), the invariant the hibernation
reconciler's tag lifecycle is designed around. Install it before (or alongside)
the operator.

The operator authenticates to the OCI registry (e.g. Artifact Registry) via
GCP ADC (Workload Identity / instance metadata) by default — no Secret to
pre-create:

```bash
# 1. Install the CRDs, RBAC, and Deployment (creates the cocoon-system namespace).
kubectl apply -k github.com/cocoonstack/cocoon-operator/config/default?ref=main

# 2. Point OCI_REGISTRY at your registry (ships empty; the manager fails fast
#    at startup until it is set).
kubectl -n cocoon-system set env deploy/cocoon-operator \
  OCI_REGISTRY=REGION-docker.pkg.dev/PROJECT/REPO
```

Clusters without ADC apply the `config/overlays/sa-key` overlay instead of
`config/default`; it mounts a service-account key from a Secret you create:

```bash
kubectl -n cocoon-system create secret generic cocoon-ar-writer-key \
  --from-file=key.json=/path/to/artifactregistry-writer-key.json
kubectl apply -k github.com/cocoonstack/cocoon-operator/config/overlays/sa-key?ref=main
```

Step 1 installs:
- `cocoon-system` namespace
- Both CRDs (imported from `cocoon-common` via `make import-crds`)
- `ServiceAccount`, `ClusterRole`, and `ClusterRoleBinding`
- The operator `Deployment` (1 replica with leader election on)

To override the image tag or replica count, build a kustomize overlay that imports `config/default` as a base.

See [Configuration](configuration.md) for the full set of environment
variables the manager reads.

## Keeping CRDs in sync with cocoon-common

The CRD YAML lives under `config/crd/bases/` and is committed so a clean clone works out of the box. After bumping the cocoon-common dependency, regenerate the bases with:

```bash
go get github.com/cocoonstack/cocoon-common@<version>
make import-crds
git add config/crd/bases && git commit
```

The `import-crds` target uses `go list -m -f '{{.Dir}}'` to resolve the cocoon-common module path and copies the YAML straight from there. CI rejects PRs that forget this step.
