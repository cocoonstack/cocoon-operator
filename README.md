# cocoon-operator

Kubernetes operator that manages VM-backed pod lifecycles through two CRDs:

- **Hibernation** -- suspend and wake a single VM-backed pod without deleting the pod object
- **CocoonSet** -- manage a group of related VM pods (main agent, sub-agents forked from it, and optional toolbox VMs)

## Why

Kubernetes controllers assume pods are replaceable. VM-backed workloads are not.

This operator keeps stateful VM workflows inside native Kubernetes APIs: scale out from a known main VM, keep stable slot identities, hibernate without letting ReplicaSets recreate the pod, and expose aggregate state through CRD status.

## Quick start

```bash
# Install CRDs
kubectl apply -f deploy/crd.yaml
kubectl apply -f deploy/cocoonset-crd.yaml

# Deploy the operator
kubectl apply -f deploy/deploy.yaml
```

### Hibernate a pod

```yaml
apiVersion: cocoon.cis/v1alpha1
kind: Hibernation
metadata:
  name: hibernate-bot-1
  namespace: prod
spec:
  podName: sre-agent-xxx
  action: hibernate
```

### Manage a VM group

```yaml
apiVersion: cocoon.cis/v1alpha1
kind: CocoonSet
metadata:
  name: demo
spec:
  agent:
    image: ubuntu-dev-base
    replicas: 1
    resources:
      cpu: "2"
      memory: "4Gi"
  toolboxes:
    - name: windows
      os: windows
      image: windows-server-2022
      mode: static
```

## Build

```bash
make build          # build binary
make test           # run tests
make lint           # run golangci-lint
make fmt            # format code
make help           # show all targets
```

Or directly:

```bash
CGO_ENABLED=0 go build -o cocoon-operator .
```

## Architecture

The operator runs a single binary with three informer loops:

1. **Hibernation controller** -- watches `Hibernation` CRDs and annotates pods with `cocoon.cis/hibernate=true` to trigger vk-cocoon snapshot/restore
2. **CocoonSet controller** -- watches `CocoonSet` CRDs, creates/deletes agent and toolbox pods, manages suspend/unsuspend, and reports aggregate status
3. **Pod watcher** -- detects pod changes owned by CocoonSets and triggers reconciliation

A periodic resync (15s) catches status transitions that informer events may miss.

## Manifests

| File | Description |
|------|-------------|
| `deploy/crd.yaml` | Hibernation CRD |
| `deploy/cocoonset-crd.yaml` | CocoonSet CRD |
| `deploy/deploy.yaml` | Operator Deployment + RBAC |

## Related components

- **vk-cocoon** -- virtual kubelet provider that performs the actual VM lifecycle
- **epoch** -- snapshot storage backend for hibernated VMs
- **glance** -- web UI that surfaces CocoonSet and Hibernation state
- **cocoon-webhook** -- optional admission webhook for sticky scheduling

## License

MIT
